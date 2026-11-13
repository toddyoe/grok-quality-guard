package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type Account struct {
	ID          string
	AuthIndex   string
	Name        string
	Type        string
	Provider    string
	Label       string
	Email       string
	Disabled    bool
	Unavailable bool
	Priority    int
	ProxyURL    string
}

type PendingObservation struct {
	AuthID    string
	RequestID string
	Body      []byte
	Analyzer  *StreamAnalyzer
	StartedAt time.Time
	Completed bool
	Source    string
	Model     string
}

type Plugin struct {
	mu                 sync.RWMutex
	cfg                Config
	policy             Policy
	yamlPolicy         Policy
	yamlRevision       string
	store              *StateStore
	stateBootstrapPath string
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	workersActive      bool
	quiescing          bool

	catalogMu sync.RWMutex
	catalog   []Account
	catalogAt time.Time

	authModelsMu sync.RWMutex
	grokModels   map[string]struct{}

	accountLocksMu sync.Mutex
	accountLocks   map[string]*sync.Mutex

	pendingMu sync.Mutex
	pending   map[string]*PendingObservation

	probeGate            sync.Mutex
	probeCond            *sync.Cond
	probeActive          int
	probePriorityWaiters int
	probing              map[string]struct{}

	probeSkipMu sync.Mutex
	probeSkip   map[string]time.Time

	recentSuccessMu sync.Mutex
	recentSuccess   map[string]time.Time

	modelStreamsMu sync.Mutex
	modelStreams   map[string]struct{}

	tasksMu sync.Mutex
	tasks   map[string]*taskRecord
	taskWG  sync.WaitGroup

	resinLeaseDeleteWake chan struct{}
}

func NewPlugin() *Plugin {
	plugin := &Plugin{
		cfg:                  DefaultConfig(),
		policy:               PolicyFromConfig(DefaultConfig()),
		accountLocks:         make(map[string]*sync.Mutex),
		pending:              make(map[string]*PendingObservation),
		probeSkip:            make(map[string]time.Time),
		recentSuccess:        make(map[string]time.Time),
		modelStreams:         make(map[string]struct{}),
		tasks:                make(map[string]*taskRecord),
		probing:              make(map[string]struct{}),
		grokModels:           make(map[string]struct{}),
		resinLeaseDeleteWake: make(chan struct{}, 1),
	}
	plugin.probeCond = sync.NewCond(&plugin.probeGate)
	return plugin
}

func (p *Plugin) markRecentSuccess(authID string) {
	if p == nil || strings.TrimSpace(authID) == "" {
		return
	}
	p.recentSuccessMu.Lock()
	p.recentSuccess[authID] = time.Now().UTC()
	p.recentSuccessMu.Unlock()
}

func (p *Plugin) recentSuccessAt(authID string) time.Time {
	if p == nil {
		return time.Time{}
	}
	p.recentSuccessMu.Lock()
	defer p.recentSuccessMu.Unlock()
	return p.recentSuccess[authID]
}

func (p *Plugin) stateStore() *StateStore {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.store
}

func (p *Plugin) Configure(raw []byte) error {
	cfg, errDecode := decodeConfig(raw)
	if errDecode != nil {
		return errDecode
	}
	revision := configRevision(raw)
	bootstrapPath := configuredStatePath(cfg.StateFile)
	activePath := bootstrapPath
	if override, errPointer := readStatePathPointer(bootstrapPath); errPointer != nil {
		return errPointer
	} else if strings.TrimSpace(override) != "" {
		normalized, errNormalize := normalizeStatePath(override)
		if errNormalize != nil {
			return fmt.Errorf("state path pointer is invalid: %w", errNormalize)
		}
		activePath = normalized
	}
	p.mu.Lock()
	oldPath := ""
	if p.store != nil {
		oldPath = p.store.Path()
	}
	if p.store == nil || oldPath != activePath || p.stateBootstrapPath != bootstrapPath {
		p.store = NewStateStore(activePath)
		if errLoad := p.store.Load(PolicyFromConfig(cfg), revision); errLoad != nil {
			p.mu.Unlock()
			return errLoad
		}
	}
	p.stateBootstrapPath = bootstrapPath
	cfg.StateFile = activePath
	p.cfg = cfg
	p.yamlPolicy = PolicyFromConfig(cfg)
	p.yamlRevision = revision
	state := p.store.Snapshot()
	if state.Policy.PassiveGuard || state.Policy.EventsLimit > 0 || state.Policy.AutoAction != "" {
		p.policy = state.Policy
	} else {
		p.policy = p.yamlPolicy
		if errUpdate := p.store.Update(func(s *State) error {
			s.Policy = p.policy
			s.PolicyYAMLRevision = revision
			return nil
		}); errUpdate != nil {
			p.mu.Unlock()
			return errUpdate
		}
	}
	if errApply := ApplyPolicy(&p.cfg, p.policy); errApply != nil {
		p.mu.Unlock()
		return errApply
	}
	p.quiescing = false
	if p.cfg.Enabled {
		p.startWorkersLocked()
	} else {
		p.stopWorkersLocked()
	}
	p.mu.Unlock()
	return nil
}

// UpdateStatePath moves the live state file and records a durable pointer next
// to the YAML/bootstrap path so the new location survives plugin restarts.
func (p *Plugin) UpdateStatePath(value string) (string, error) {
	target, errNormalize := normalizeStatePath(value)
	if errNormalize != nil {
		return "", errNormalize
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.store
	if store == nil {
		return "", errors.New("state store is not initialized")
	}
	oldPath := store.Path()
	if normalizedOldPath, errOld := normalizeStatePath(oldPath); errOld == nil && normalizedOldPath == target {
		p.cfg.StateFile = target
		return target, nil
	}
	bootstrapPath := strings.TrimSpace(p.stateBootstrapPath)
	if bootstrapPath == "" {
		bootstrapPath = oldPath
	}
	if errSwitch := store.SetPath(target); errSwitch != nil {
		return "", errSwitch
	}
	if errPointer := writeStatePathPointer(bootstrapPath, target); errPointer != nil {
		if errRestore := store.setPath(oldPath, true); errRestore != nil {
			return "", errors.Join(errPointer, fmt.Errorf("restore previous state path: %w", errRestore))
		}
		return "", errPointer
	}
	p.cfg.StateFile = target
	return target, nil
}

func (p *Plugin) startWorkersLocked() {
	if p.workersActive || !p.cfg.Enabled {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel
	p.workersActive = true
	p.wg.Add(3)
	go func() {
		defer p.wg.Done()
		p.refreshLoop(ctx)
	}()
	go func() {
		defer p.wg.Done()
		p.probeLoop(ctx)
	}()
	go func() {
		defer p.wg.Done()
		p.resinLeaseDeleteLoop(ctx)
	}()
}

func (p *Plugin) stopWorkersLocked() {
	if p.cancel != nil {
		p.cancel()
	}
	p.cancel = nil
	p.ctx = nil
	p.workersActive = false
}

func (p *Plugin) Quiesce() {
	p.mu.Lock()
	p.quiescing = true
	p.stopWorkersLocked()
	p.mu.Unlock()
	p.cancelTasks()
	p.closeAllModelStreams()
}

func (p *Plugin) Shutdown() {
	p.mu.Lock()
	p.stopWorkersLocked()
	p.mu.Unlock()
	p.cancelTasks()
	p.closeAllModelStreams()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		p.taskWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	if p.stateStore() != nil {
		_ = p.stateStore().Update(func(*State) error { return nil })
	}
}

func (p *Plugin) refreshLoop(ctx context.Context) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	_ = p.RefreshCatalog(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = p.RefreshCatalog(ctx)
		}
	}
}

func (p *Plugin) RefreshCatalog(_ context.Context) error {
	entries, errList := p.listAuths()
	if errList != nil {
		return errList
	}
	accounts := make([]Account, 0, len(entries))
	for _, entry := range entries {
		provider := strings.TrimSpace(entry.Provider)
		if provider == "" {
			provider = strings.TrimSpace(entry.Type)
		}
		if provider != "" && !isGrokProvider(provider) && !isGrokProvider(entry.Type) {
			continue
		}
		id := strings.TrimSpace(entry.ID)
		if id == "" {
			id = strings.TrimSpace(entry.AuthIndex)
		}
		if id == "" {
			continue
		}
		account := Account{
			ID:          id,
			AuthIndex:   strings.TrimSpace(entry.AuthIndex),
			Name:        entry.Name,
			Type:        entry.Type,
			Provider:    provider,
			Label:       entry.Label,
			Email:       entry.Email,
			Disabled:    entry.Disabled,
			Unavailable: entry.Unavailable,
			Priority:    entry.Priority,
		}
		accounts = append(accounts, account)
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].Priority != accounts[j].Priority {
			return accounts[i].Priority < accounts[j].Priority
		}
		return accounts[i].ID < accounts[j].ID
	})
	p.catalogMu.Lock()
	p.catalog = accounts
	p.catalogAt = time.Now().UTC()
	p.catalogMu.Unlock()
	if authModels, errModels := p.listAuthModels(); errModels == nil {
		p.setGrokAuthModels(authModels)
	}
	_ = p.syncCatalogState(accounts)
	return nil
}

func (p *Plugin) syncCatalogState(accounts []Account) error {
	if p.stateStore() == nil {
		return nil
	}
	now := time.Now().UTC()
	live := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		live[account.ID] = struct{}{}
	}
	return p.stateStore().UpdateIfChanged(func(state *State) error {
		for authID := range state.Accounts {
			if _, ok := live[authID]; !ok {
				delete(state.Accounts, authID)
			}
		}
		for _, account := range accounts {
			existing := state.Accounts[account.ID]
			if existing == nil {
				existing = &AccountState{
					AuthID:      account.ID,
					State:       StateUnknown,
					FirstSeenAt: now,
				}
				state.Accounts[account.ID] = existing
			}
			if account.Email != "" {
				existing.Email = account.Email
			}
			if account.Label != "" {
				existing.Label = account.Label
			}
			if account.Disabled {
				switch existing.State {
				case StateCooling, StateQuotaExhausted, StateCredentialInvalid, StateDisabledByGuard:
				default:
					existing.State = StateExternallyDisabled
				}
			}
			if !account.Disabled && existing.State == StateExternallyDisabled {
				existing.State = StateUnknown
			}
		}
		return nil
	})
}

func (p *Plugin) catalogSnapshot() []Account {
	p.catalogMu.RLock()
	defer p.catalogMu.RUnlock()
	return append([]Account(nil), p.catalog...)
}

func (p *Plugin) findAccount(authID string) (Account, bool) {
	normalized := strings.TrimSpace(authID)
	if normalized == "" {
		return Account{}, false
	}
	for _, account := range p.catalogSnapshot() {
		if account.ID == normalized || account.AuthIndex == normalized {
			return account, true
		}
	}
	return Account{}, false
}

func (p *Plugin) resolveAuth(ctx context.Context, authID string) (Account, pluginapi.HostAuthGetResponse, map[string]json.RawMessage, error) {
	account, okAccount := p.findAccount(authID)
	if !okAccount {
		if errRefresh := p.RefreshCatalog(ctx); errRefresh != nil {
			return Account{}, pluginapi.HostAuthGetResponse{}, nil, errRefresh
		}
		account, okAccount = p.findAccount(authID)
	}
	if !okAccount {
		return Account{}, pluginapi.HostAuthGetResponse{}, nil, errors.New("auth not found")
	}
	index := account.AuthIndex
	if index == "" {
		index = account.ID
	}
	response, errGet := p.getAuth(index)
	if errGet != nil {
		return account, response, nil, errGet
	}
	raw, errMap := rawAuthToMap(response.JSON)
	if errMap != nil {
		return account, response, nil, errMap
	}
	return account, response, raw, nil
}

func (p *Plugin) accountLock(authID string) *sync.Mutex {
	p.accountLocksMu.Lock()
	defer p.accountLocksMu.Unlock()
	lock := p.accountLocks[authID]
	if lock == nil {
		lock = &sync.Mutex{}
		p.accountLocks[authID] = lock
	}
	return lock
}

func (p *Plugin) ensureStateAccount(state *State, authID, email, label string) *AccountState {
	if state.Accounts == nil {
		state.Accounts = make(map[string]*AccountState)
	}
	account := state.Accounts[authID]
	if account == nil {
		account = &AccountState{AuthID: authID, State: StateUnknown, FirstSeenAt: time.Now().UTC()}
		state.Accounts[authID] = account
	}
	if email != "" {
		account.Email = email
	}
	if label != "" {
		account.Label = label
	}
	if account.FirstSeenAt.IsZero() {
		account.FirstSeenAt = time.Now().UTC()
	}
	return account
}

func (p *Plugin) stateAccount(authID string) (AccountState, bool) {
	store := p.stateStore()
	if store == nil {
		return AccountState{}, false
	}
	return store.AccountSnapshot(authID)
}

func (p *Plugin) updateStateAccount(authID string, fn func(*AccountState) error) error {
	if p.stateStore() == nil {
		return errors.New("state store is not initialized")
	}
	return p.stateStore().UpdateIfChanged(func(state *State) error {
		account := p.ensureStateAccount(state, authID, "", "")
		return fn(account)
	})
}

func (p *Plugin) recordEvent(state *State, authID, class, action, result, detail string) {
	appendEvent(state, Event{
		Time:   time.Now().UTC(),
		AuthID: authID,
		Class:  class,
		Action: action,
		Result: result,
		Detail: detail,
	})
}

func (p *Plugin) isProbeAuth(authID string) bool {
	p.probeSkipMu.Lock()
	defer p.probeSkipMu.Unlock()
	now := time.Now()
	for key, expiry := range p.probeSkip {
		if now.After(expiry) {
			delete(p.probeSkip, key)
		}
	}
	_, ok := p.probeSkip[authID]
	return ok
}

func (p *Plugin) markProbeAuth(authID string, ttl time.Duration) {
	if authID == "" {
		return
	}
	p.probeSkipMu.Lock()
	p.probeSkip[authID] = time.Now().Add(ttl)
	p.probeSkipMu.Unlock()
}
func (p *Plugin) trackModelStream(streamID string) {
	if p == nil || strings.TrimSpace(streamID) == "" {
		return
	}
	p.modelStreamsMu.Lock()
	p.modelStreams[streamID] = struct{}{}
	p.modelStreamsMu.Unlock()
}

func (p *Plugin) untrackModelStream(streamID string) {
	if p == nil || strings.TrimSpace(streamID) == "" {
		return
	}
	p.modelStreamsMu.Lock()
	delete(p.modelStreams, streamID)
	p.modelStreamsMu.Unlock()
}

func (p *Plugin) closeAllModelStreams() {
	if p == nil {
		return
	}
	p.modelStreamsMu.Lock()
	streamIDs := make([]string, 0, len(p.modelStreams))
	for streamID := range p.modelStreams {
		streamIDs = append(streamIDs, streamID)
	}
	p.modelStreamsMu.Unlock()
	for _, streamID := range streamIDs {
		_ = p.closeModelStream(streamID)
	}
}
