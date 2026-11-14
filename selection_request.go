package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	transparentSourceSession   = "session"
	transparentSourcePreferred = "preferred"
	transparentSourceStrategy  = "strategy"
)

const (
	selectionNormalIdlePass = iota
	selectionNormalBusyPass
)

type transparentSelection struct {
	plugin  *Plugin
	routing RoutingConfig

	sessionKey         string
	sessionAuthID      string
	preferredID        string
	snapshot           *routingSelectionSnapshot
	topLayerKey        string
	layerOrder         []string
	layerIndex         int
	currentLayerKey    string
	currentLayer       []Account
	currentLayerLoaded bool
	specialsDone       bool
	normalStart        int
	normalOffset       int
	normalPass         int
	tried              map[string]struct{}
	totalCandidates    int
}

type transparentCandidateAttempt struct {
	selection *transparentSelection
	Account   Account
	Raw       map[string]json.RawMessage
	Source    string
	released  bool
}

func (attempt *transparentCandidateAttempt) Release() {
	if attempt == nil || attempt.released {
		return
	}
	attempt.selection.plugin.selectionRuntime.ReleaseAttempt(attempt.Account.ID)
	attempt.released = true
}

func (p *Plugin) newTransparentSelection(req rpcExecutorRequest, body []byte, sourceFormat string) (*transparentSelection, error) {
	if errEnsure := p.ensureSelectionIndex(); errEnsure != nil {
		return nil, errEnsure
	}
	p.mu.RLock()
	policy := p.policy
	p.mu.RUnlock()
	snapshot := p.selectionIndex.Load()
	if snapshot == nil {
		return nil, fmt.Errorf("selection index is not ready")
	}
	selection := &transparentSelection{
		plugin:          p,
		routing:         policy.Routing,
		snapshot:        snapshot,
		layerOrder:      snapshot.Order(),
		layerIndex:      0,
		normalStart:     -1,
		tried:           make(map[string]struct{}),
		totalCandidates: snapshot.Total(),
	}
	for _, key := range selection.layerOrder {
		if len(snapshot.Layer(key)) > 0 {
			selection.topLayerKey = key
			break
		}
	}
	if selection.routing.SessionAffinity {
		selection.sessionKey = p.selectionSessionKey(req, body, sourceFormat)
		if selection.sessionKey != "" {
			if authID, ok := p.selectionRuntime.LookupBinding(selection.sessionKey, time.Now().UTC()); ok {
				selection.sessionAuthID = authID
			}
		}
	}
	if selection.routing.PreferLastSuccess {
		if authID, _ := p.selectionRuntime.Preferred(); authID != "" {
			layerKey, ok := snapshot.LayerForAuth(authID)
			if !ok || layerKey != selection.topLayerKey {
				p.selectionRuntime.ClearPreferred(authID)
			} else {
				selection.preferredID = authID
			}
		}
	}
	return selection, nil
}

func (selection *transparentSelection) Next(ctx context.Context) (transparentCandidateAttempt, bool) {
	if selection == nil || selection.totalCandidates <= 0 {
		return transparentCandidateAttempt{}, false
	}
	for {
		if !selection.currentLayerLoaded {
			if selection.layerIndex >= len(selection.layerOrder) {
				return transparentCandidateAttempt{}, false
			}
			key := selection.layerOrder[selection.layerIndex]
			layer := selection.snapshot.Layer(key)
			if len(layer) == 0 {
				selection.layerIndex++
				continue
			}
			selection.currentLayerKey = key
			selection.currentLayer = layer
			selection.currentLayerLoaded = true
			selection.specialsDone = false
			selection.normalStart = -1
			selection.normalOffset = 0
			selection.normalPass = selectionNormalIdlePass
		}
		if !selection.specialsDone {
			selection.specialsDone = true
			if attempt, ok := selection.trySessionSpecial(ctx); ok {
				return attempt, true
			}
			if attempt, ok := selection.tryPreferredSpecial(ctx); ok {
				return attempt, true
			}
		}
		if attempt, ok := selection.tryNormalCandidate(ctx); ok {
			return attempt, true
		}
		selection.currentLayerLoaded = false
		selection.layerIndex++
	}
}

func (selection *transparentSelection) tryNormalCandidate(ctx context.Context) (transparentCandidateAttempt, bool) {
	if selection.normalStart < 0 {
		selection.normalStart = selection.plugin.selectionRuntime.StrategyStart(
			selection.currentLayerKey,
			selection.routing.SelectionStrategy,
			len(selection.currentLayer),
		)
	}
	for selection.normalPass <= selectionNormalBusyPass {
		for selection.normalOffset < len(selection.currentLayer) {
			position := (selection.normalStart + selection.normalOffset) % len(selection.currentLayer)
			selection.normalOffset++
			candidate := selection.currentLayer[position]
			if _, tried := selection.tried[candidate.ID]; tried {
				continue
			}
			if !selection.candidateEligible(candidate.ID) {
				selection.tried[candidate.ID] = struct{}{}
				selection.plugin.refreshSelectionAccount(candidate.ID)
				continue
			}
			if selection.normalPass == selectionNormalIdlePass {
				if !selection.plugin.selectionRuntime.TryReserveIdle(candidate.ID) {
					continue
				}
			} else {
				selection.plugin.selectionRuntime.ReserveAttempt(candidate.ID)
			}
			account, raw, ok := selection.plugin.executableAuth(ctx, candidate.ID)
			if !ok {
				selection.plugin.selectionRuntime.ReleaseAttempt(candidate.ID)
				selection.tried[candidate.ID] = struct{}{}
				selection.plugin.refreshSelectionAccount(candidate.ID)
				continue
			}
			attempt := transparentCandidateAttempt{
				selection: selection,
				Account:   account,
				Raw:       raw,
				Source:    transparentSourceStrategy,
			}
			selection.applySessionBinding(ctx, &attempt)
			selection.tried[attempt.Account.ID] = struct{}{}
			return attempt, true
		}
		if selection.normalPass == selectionNormalIdlePass {
			selection.normalPass = selectionNormalBusyPass
			selection.normalOffset = 0
			continue
		}
		break
	}
	return transparentCandidateAttempt{}, false
}

func (selection *transparentSelection) applySessionBinding(ctx context.Context, attempt *transparentCandidateAttempt) {
	if selection.sessionKey == "" || attempt == nil {
		return
	}
	boundID := selection.plugin.selectionRuntime.GetOrBind(
		selection.sessionKey,
		attempt.Account.ID,
		time.Now().UTC(),
		selection.routing.SessionAffinityTTL.Duration,
		selection.routing.SessionAffinityMaxSize,
	)
	if boundID == "" || boundID == attempt.Account.ID {
		return
	}
	boundLayer, exists := selection.snapshot.LayerForAuth(boundID)
	if !exists || !selection.candidateEligible(boundID) {
		selection.plugin.selectionRuntime.DeleteIfBoundTo(selection.sessionKey, boundID)
		return
	}
	if boundLayer != selection.currentLayerKey {
		// Keep a valid lower-layer binding until this request falls back there
		return
	}
	if _, tried := selection.tried[boundID]; tried {
		return
	}
	boundAccount, boundRaw, executable := selection.plugin.executableAuth(ctx, boundID)
	if !executable {
		selection.plugin.selectionRuntime.DeleteIfBoundTo(selection.sessionKey, boundID)
		return
	}
	selection.plugin.selectionRuntime.ReleaseAttempt(attempt.Account.ID)
	// Session affinity intentionally permits more than one in-flight request
	// to use the bound auth so a conversation is not split across accounts
	selection.plugin.selectionRuntime.ReserveAttempt(boundID)
	attempt.Account = boundAccount
	attempt.Raw = boundRaw
	attempt.Source = transparentSourceSession
}

func (selection *transparentSelection) trySessionSpecial(ctx context.Context) (transparentCandidateAttempt, bool) {
	if selection.sessionAuthID == "" {
		return transparentCandidateAttempt{}, false
	}
	layerKey, ok := selection.snapshot.LayerForAuth(selection.sessionAuthID)
	if !ok {
		selection.plugin.selectionRuntime.DeleteIfBoundTo(selection.sessionKey, selection.sessionAuthID)
		selection.sessionAuthID = ""
		return transparentCandidateAttempt{}, false
	}
	if layerKey != selection.currentLayerKey {
		return transparentCandidateAttempt{}, false
	}
	authID := selection.sessionAuthID
	if _, tried := selection.tried[authID]; tried {
		return transparentCandidateAttempt{}, false
	}
	selection.tried[authID] = struct{}{}
	if !selection.candidateEligible(authID) {
		selection.plugin.selectionRuntime.DeleteIfBoundTo(selection.sessionKey, authID)
		selection.sessionAuthID = ""
		selection.plugin.refreshSelectionAccount(authID)
		return transparentCandidateAttempt{}, false
	}
	account, raw, ok := selection.plugin.executableAuth(ctx, authID)
	if !ok {
		selection.plugin.selectionRuntime.DeleteIfBoundTo(selection.sessionKey, authID)
		selection.sessionAuthID = ""
		selection.plugin.refreshSelectionAccount(authID)
		return transparentCandidateAttempt{}, false
	}
	selection.plugin.selectionRuntime.ReserveAttempt(account.ID)
	return transparentCandidateAttempt{
		selection: selection,
		Account:   account,
		Raw:       raw,
		Source:    transparentSourceSession,
	}, true
}

func (selection *transparentSelection) tryPreferredSpecial(ctx context.Context) (transparentCandidateAttempt, bool) {
	if selection.preferredID == "" || !selection.routing.PreferLastSuccess {
		return transparentCandidateAttempt{}, false
	}
	if selection.currentLayerKey != selection.topLayerKey {
		return transparentCandidateAttempt{}, false
	}
	authID := selection.preferredID
	layerKey, ok := selection.snapshot.LayerForAuth(authID)
	if !ok || layerKey != selection.topLayerKey {
		selection.plugin.selectionRuntime.ClearPreferred(authID)
		selection.preferredID = ""
		return transparentCandidateAttempt{}, false
	}
	if _, tried := selection.tried[authID]; tried {
		return transparentCandidateAttempt{}, false
	}
	state, okState := selection.plugin.stateAccount(authID)
	if !okState || state.State != StateHealthy {
		selection.plugin.selectionRuntime.ClearPreferred(authID)
		selection.preferredID = ""
		return transparentCandidateAttempt{}, false
	}
	if !selection.plugin.selectionRuntime.TryReservePreferred(authID) {
		return transparentCandidateAttempt{}, false
	}
	selection.tried[authID] = struct{}{}
	account, raw, ok := selection.plugin.executableAuth(ctx, authID)
	if !ok {
		selection.plugin.selectionRuntime.ReleaseAttempt(authID)
		selection.plugin.selectionRuntime.ClearPreferred(authID)
		selection.preferredID = ""
		selection.plugin.refreshSelectionAccount(authID)
		return transparentCandidateAttempt{}, false
	}
	return transparentCandidateAttempt{
		selection: selection,
		Account:   account,
		Raw:       raw,
		Source:    transparentSourcePreferred,
	}, true
}

func (selection *transparentSelection) candidateEligible(authID string) bool {
	account, ok := selection.plugin.findAccount(authID)
	if !ok {
		return false
	}
	state, okState := selection.plugin.stateAccount(authID)
	if !okState {
		return accountEligibleForSelection(account, StateUnknown)
	}
	return accountEligibleForSelection(account, state.State)
}

func (selection *transparentSelection) preferredEligible(authID string) bool {
	layerKey, ok := selection.snapshot.LayerForAuth(authID)
	if !ok || layerKey != selection.topLayerKey {
		return false
	}
	state, ok := selection.plugin.stateAccount(authID)
	return ok && state.State == StateHealthy && selection.candidateEligible(authID)
}

func (selection *transparentSelection) MarkSuccess(authID, source string) {
	if selection == nil || authID == "" {
		return
	}
	now := time.Now().UTC()
	if selection.routing.SessionAffinity && selection.sessionKey != "" {
		selection.plugin.selectionRuntime.RefreshBinding(
			selection.sessionKey,
			authID,
			now,
			selection.routing.SessionAffinityTTL.Duration,
		)
	}
	switch source {
	case transparentSourcePreferred:
		selection.plugin.selectionRuntime.RefreshPreferred(authID, now)
	case transparentSourceStrategy:
		if !selection.routing.PreferLastSuccess {
			return
		}
		currentPreferred, _ := selection.plugin.selectionRuntime.Preferred()
		if currentPreferred == "" || currentPreferred == authID || !selection.preferredEligible(currentPreferred) {
			selection.plugin.selectionRuntime.SetPreferred(authID, now)
			selection.preferredID = authID
		}
	}
}

func (selection *transparentSelection) MarkFailure(authID, _ string, class string) {
	if selection == nil || authID == "" || !isTransparentSelectionHardFailure(class) {
		return
	}
	if selection.sessionKey != "" {
		selection.plugin.selectionRuntime.DeleteIfBoundTo(selection.sessionKey, authID)
		if selection.sessionAuthID == authID {
			selection.sessionAuthID = ""
		}
	}
	if selection.preferredID == authID {
		selection.plugin.selectionRuntime.ClearPreferred(authID)
		selection.preferredID = ""
	}
}

func isTransparentSelectionHardFailure(class string) bool {
	switch class {
	case "", ClassIgnored, ClassProbeError, ClassProxyError:
		return false
	default:
		return true
	}
}

func (p *Plugin) transparentCandidates(ctx context.Context) ([]Account, error) {
	if errEnsure := p.ensureSelectionIndex(); errEnsure != nil {
		return nil, errEnsure
	}
	// Diagnostic/test helper only. Request execution uses the incremental selector.
	_ = ctx
	_ = p.rebuildSelectionIndex()
	candidates := make([]Account, 0, p.selectionIndex.Total())
	for _, key := range p.selectionIndex.Order() {
		candidates = append(candidates, p.selectionIndex.Layer(key)...)
	}
	return candidates, nil
}
