package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	resinLeaseDeleteScanInterval = 5 * time.Second
	resinLeaseDeleteRetryBase    = 30 * time.Second
	resinLeaseDeleteRetryMax     = time.Hour
)

func resinLeaseDeleteKey(adminURL, platformID, account string) string {
	return strings.TrimRight(strings.TrimSpace(adminURL), "/") + "\x00" +
		strings.TrimSpace(platformID) + "\x00" + strings.TrimSpace(account)
}

func queueResinLeaseDelete(state *State, authID string, settings ResinSettings, account string) bool {
	account = strings.TrimSpace(account)
	if state == nil || account == "" {
		return false
	}
	key := resinLeaseDeleteKey(settings.AdminURL, settings.PlatformID, account)
	for index := range state.ResinLeaseDeletes {
		job := state.ResinLeaseDeletes[index]
		if resinLeaseDeleteKey(job.AdminURL, job.PlatformID, job.Account) == key {
			return false
		}
	}
	now := time.Now().UTC()
	state.ResinLeaseDeletes = append(state.ResinLeaseDeletes, ResinLeaseDelete{
		AuthID:        strings.TrimSpace(authID),
		AdminURL:      strings.TrimSpace(settings.AdminURL),
		AdminToken:    strings.TrimSpace(settings.AdminToken),
		PlatformID:    strings.TrimSpace(settings.PlatformID),
		Account:       account,
		CreatedAt:     now,
		NextAttemptAt: now,
	})
	return true
}

func queueResinLeaseDeleteAndRecord(state *State, authID string, settings ResinSettings, account string) bool {
	added := queueResinLeaseDelete(state, authID, settings, account)
	if added {
		appendEvent(state, Event{
			Time:   time.Now().UTC(),
			AuthID: strings.TrimSpace(authID),
			Action: "resin_lease_delete",
			Result: "queued",
			Detail: "account=" + strings.TrimSpace(account),
		})
	}
	return added
}

func (p *Plugin) enqueueResinLeaseDelete(authID string, settings ResinSettings, account string) (bool, error) {
	if p == nil || p.stateStore() == nil {
		return false, errors.New("state store is not initialized")
	}
	added := false
	errUpdate := p.stateStore().UpdateIfChanged(func(state *State) error {
		added = queueResinLeaseDeleteAndRecord(state, authID, settings, account)
		return nil
	})
	if errUpdate != nil {
		return false, errUpdate
	}
	if added {
		p.wakeResinLeaseDeleteLoop()
	}
	return added, nil
}

func (p *Plugin) wakeResinLeaseDeleteLoop() {
	if p == nil || p.resinLeaseDeleteWake == nil {
		return
	}
	select {
	case p.resinLeaseDeleteWake <- struct{}{}:
	default:
	}
}

func (p *Plugin) resinLeaseDeleteLoop(ctx context.Context) {
	if p == nil || ctx == nil {
		return
	}
	_ = p.processDueResinLeaseDeletes(ctx, time.Now().UTC())
	ticker := time.NewTicker(resinLeaseDeleteScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.resinLeaseDeleteWake:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		_ = p.processDueResinLeaseDeletes(ctx, time.Now().UTC())
	}
}

func (p *Plugin) processDueResinLeaseDeletes(ctx context.Context, now time.Time) int {
	if p == nil || p.stateStore() == nil {
		return 0
	}
	if ctx == nil {
		ctx = context.Background()
	}
	state := p.stateStore().Snapshot()
	processed := 0
	for _, job := range state.ResinLeaseDeletes {
		if ctx.Err() != nil {
			break
		}
		if !job.NextAttemptAt.IsZero() && job.NextAttemptAt.After(now) {
			continue
		}
		p.processResinLeaseDelete(ctx, job)
		processed++
	}
	return processed
}

func sameResinAdminURL(left, right string) bool {
	left = strings.TrimRight(strings.TrimSpace(left), "/")
	right = strings.TrimRight(strings.TrimSpace(right), "/")
	return left != "" && right != "" && strings.EqualFold(left, right)
}

func (p *Plugin) processResinLeaseDelete(ctx context.Context, job ResinLeaseDelete) {
	if ctx == nil {
		ctx = context.Background()
	}
	settings := p.ResinConfig()
	adminURL := firstNonEmpty(job.AdminURL, settings.AdminURL)
	platformID := firstNonEmpty(job.PlatformID, settings.PlatformID)
	adminToken := strings.TrimSpace(job.AdminToken)
	if adminToken == "" || sameResinAdminURL(adminURL, settings.AdminURL) {
		adminToken = firstNonEmpty(strings.TrimSpace(settings.AdminToken), adminToken)
	}
	var errDelete error
	switch {
	case strings.TrimSpace(adminURL) == "":
		errDelete = errors.New("resin admin URL is missing")
	case strings.TrimSpace(platformID) == "":
		errDelete = errors.New("resin platform ID is missing")
	case strings.TrimSpace(adminToken) == "":
		errDelete = errors.New("resin admin token is missing")
	default:
		errDelete = deleteResinLease(ctx, adminURL, platformID, adminToken, job.Account)
	}
	if ctx.Err() != nil {
		return
	}
	if errDelete == nil {
		p.finishResinLeaseDelete(job)
		return
	}
	p.rescheduleResinLeaseDelete(job, errDelete, time.Now().UTC())
}

func (p *Plugin) finishResinLeaseDelete(job ResinLeaseDelete) {
	_ = p.stateStore().UpdateIfChanged(func(state *State) error {
		removed := removeResinLeaseDelete(state, job)
		if removed {
			p.recordEvent(state, job.AuthID, "", "resin_lease_delete", "succeeded", "account="+job.Account)
		}
		return nil
	})
}

func (p *Plugin) rescheduleResinLeaseDelete(job ResinLeaseDelete, errDelete error, now time.Time) {
	attempts := job.Attempts + 1
	delay := resinLeaseDeleteBackoff(attempts)
	nextAttemptAt := now.Add(delay)
	reason := sanitizeError(errDelete)
	_ = p.stateStore().UpdateIfChanged(func(state *State) error {
		for index := range state.ResinLeaseDeletes {
			candidate := &state.ResinLeaseDeletes[index]
			if resinLeaseDeleteKey(candidate.AdminURL, candidate.PlatformID, candidate.Account) !=
				resinLeaseDeleteKey(job.AdminURL, job.PlatformID, job.Account) {
				continue
			}
			candidate.Attempts = attempts
			candidate.NextAttemptAt = nextAttemptAt
			candidate.LastError = reason
			p.recordEvent(state, candidate.AuthID, "", "resin_lease_delete", "retry_scheduled",
				fmt.Sprintf("account=%s attempt=%d retry_in=%s error=%s", candidate.Account, attempts, delay.Round(time.Second), reason))
			break
		}
		return nil
	})
}

func removeResinLeaseDelete(state *State, job ResinLeaseDelete) bool {
	if state == nil {
		return false
	}
	key := resinLeaseDeleteKey(job.AdminURL, job.PlatformID, job.Account)
	for index := range state.ResinLeaseDeletes {
		candidate := state.ResinLeaseDeletes[index]
		if resinLeaseDeleteKey(candidate.AdminURL, candidate.PlatformID, candidate.Account) != key {
			continue
		}
		state.ResinLeaseDeletes = append(state.ResinLeaseDeletes[:index], state.ResinLeaseDeletes[index+1:]...)
		return true
	}
	return false
}

func resinLeaseDeleteBackoff(attempts int) time.Duration {
	if attempts <= 1 {
		return resinLeaseDeleteRetryBase
	}
	delay := resinLeaseDeleteRetryBase
	for attempt := 1; attempt < attempts; attempt++ {
		if delay >= resinLeaseDeleteRetryMax/2 {
			return resinLeaseDeleteRetryMax
		}
		delay *= 2
	}
	if delay > resinLeaseDeleteRetryMax {
		return resinLeaseDeleteRetryMax
	}
	return delay
}
