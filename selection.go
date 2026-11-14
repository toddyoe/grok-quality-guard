package main

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type routingSelectionLayer struct {
	Key         string
	QualityRank int
	Priority    int
	Accounts    []Account
}

type routingSelectionSnapshot struct {
	layers      map[string]routingSelectionLayer
	layerByAuth map[string]string
	order       []string
	total       int
}

type routingSelectionIndex struct {
	buildMu         sync.Mutex
	snapshot        atomic.Pointer[routingSelectionSnapshot]
	buildGeneration atomic.Uint64
}

type routingSelectionRuntime struct {
	mu                 sync.Mutex
	cursors            map[string]int
	activeAttempts     map[string]int
	preferredAuthID    string
	preferredSuccessAt time.Time
	bindings           map[string]routingSessionBinding
}

type routingSessionBinding struct {
	AuthID    string
	ExpiresAt time.Time
	BoundAt   time.Time
}

func newRoutingSelectionRuntime() routingSelectionRuntime {
	return routingSelectionRuntime{
		cursors:        make(map[string]int),
		activeAttempts: make(map[string]int),
		bindings:       make(map[string]routingSessionBinding),
	}
}

func selectionLayerForState(state string, priority int) (string, int, bool) {
	switch state {
	case "", StateUnknown:
		return fmt.Sprintf("1:%d", priority), 1, true
	case StateHealthy:
		return fmt.Sprintf("0:%d", priority), 0, true
	default:
		return "", 0, false
	}
}

func selectionStateForAccount(states map[string]*AccountState, account Account) *AccountState {
	if value := states[account.ID]; value != nil {
		return value
	}
	authIndex := strings.TrimSpace(account.AuthIndex)
	if authIndex != "" && authIndex != account.ID {
		return states[authIndex]
	}
	return nil
}

func accountEligibleForSelection(account Account, state string) bool {
	if account.Disabled || account.Unavailable {
		return false
	}
	if account.Provider != "" && !isGrokProvider(account.Provider) && !isGrokProvider(account.Type) {
		return false
	}
	return isTransparentCandidateState(state)
}

func (index *routingSelectionIndex) Load() *routingSelectionSnapshot {
	if index == nil {
		return nil
	}
	return index.snapshot.Load()
}

func (index *routingSelectionIndex) Ready() bool {
	return index.Load() != nil
}

func (index *routingSelectionIndex) BuildGeneration() uint64 {
	if index == nil {
		return 0
	}
	return index.buildGeneration.Load()
}

func (snapshot *routingSelectionSnapshot) Order() []string {
	if snapshot == nil {
		return nil
	}
	return snapshot.order
}

func (snapshot *routingSelectionSnapshot) Layer(key string) []Account {
	if snapshot == nil {
		return nil
	}
	layer := snapshot.layers[key]
	return layer.Accounts
}

func (snapshot *routingSelectionSnapshot) LayerForAuth(authID string) (string, bool) {
	if snapshot == nil {
		return "", false
	}
	key, ok := snapshot.layerByAuth[authID]
	return key, ok
}

func (snapshot *routingSelectionSnapshot) Total() int {
	if snapshot == nil {
		return 0
	}
	return snapshot.total
}

func (index *routingSelectionIndex) Order() []string {
	return index.Load().Order()
}

func (index *routingSelectionIndex) Layer(key string) []Account {
	return index.Load().Layer(key)
}

func (index *routingSelectionIndex) LayerForAuth(authID string) (string, bool) {
	return index.Load().LayerForAuth(authID)
}

func (index *routingSelectionIndex) Total() int {
	return index.Load().Total()
}

func (index *routingSelectionIndex) Build(accounts []Account, states map[string]*AccountState) {
	groups := make(map[string][]Account)
	meta := make(map[string]routingSelectionLayer)
	for _, account := range accounts {
		state := StateUnknown
		if value := selectionStateForAccount(states, account); value != nil && value.State != "" {
			state = value.State
		}
		if !accountEligibleForSelection(account, state) {
			continue
		}
		key, rank, ok := selectionLayerForState(state, account.Priority)
		if !ok {
			continue
		}
		if _, exists := meta[key]; !exists {
			meta[key] = routingSelectionLayer{Key: key, QualityRank: rank, Priority: account.Priority}
		}
		groups[key] = append(groups[key], account)
	}

	layers := make(map[string]routingSelectionLayer, len(groups))
	layerByAuth := make(map[string]string, len(accounts))
	for key, values := range groups {
		sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
		layer := meta[key]
		layer.Accounts = values
		layers[key] = layer
		for _, account := range values {
			layerByAuth[account.ID] = key
		}
	}
	order, total := buildSelectionOrder(layers)
	index.snapshot.Store(&routingSelectionSnapshot{
		layers:      layers,
		layerByAuth: layerByAuth,
		order:       order,
		total:       total,
	})
	index.buildGeneration.Add(1)
}

// Upsert publishes a copy-on-write snapshot and must be called while buildMu is held
func (index *routingSelectionIndex) Upsert(account Account, state AccountState) {
	current := index.Load()
	if current == nil {
		return
	}
	layers := make(map[string]routingSelectionLayer, len(current.layers)+1)
	for key, layer := range current.layers {
		layers[key] = layer
	}
	layerByAuth := make(map[string]string, len(current.layerByAuth)+1)
	for authID, key := range current.layerByAuth {
		layerByAuth[authID] = key
	}
	if oldKey, ok := layerByAuth[account.ID]; ok {
		delete(layerByAuth, account.ID)
		layer := layers[oldKey]
		accounts := removeAccountFromLayer(layer.Accounts, account.ID)
		if len(accounts) == 0 {
			delete(layers, oldKey)
		} else {
			layer.Accounts = accounts
			layers[oldKey] = layer
		}
	}
	stateValue := state.State
	if stateValue == "" {
		stateValue = StateUnknown
	}
	if accountEligibleForSelection(account, stateValue) {
		key, rank, ok := selectionLayerForState(stateValue, account.Priority)
		if ok {
			layer := layers[key]
			layer.Key = key
			layer.QualityRank = rank
			layer.Priority = account.Priority
			layer.Accounts = insertAccountSorted(layer.Accounts, account)
			layers[key] = layer
			layerByAuth[account.ID] = key
		}
	}
	order, total := buildSelectionOrder(layers)
	index.snapshot.Store(&routingSelectionSnapshot{
		layers:      layers,
		layerByAuth: layerByAuth,
		order:       order,
		total:       total,
	})
}

// Remove publishes a copy-on-write snapshot and must be called while buildMu is held
func (index *routingSelectionIndex) Remove(authID string) {
	current := index.Load()
	if current == nil {
		return
	}
	oldKey, ok := current.layerByAuth[authID]
	if !ok {
		return
	}
	layers := make(map[string]routingSelectionLayer, len(current.layers))
	for key, layer := range current.layers {
		layers[key] = layer
	}
	layerByAuth := make(map[string]string, len(current.layerByAuth))
	for id, key := range current.layerByAuth {
		if id != authID {
			layerByAuth[id] = key
		}
	}
	layer := layers[oldKey]
	accounts := removeAccountFromLayer(layer.Accounts, authID)
	if len(accounts) == 0 {
		delete(layers, oldKey)
	} else {
		layer.Accounts = accounts
		layers[oldKey] = layer
	}
	order, total := buildSelectionOrder(layers)
	index.snapshot.Store(&routingSelectionSnapshot{
		layers:      layers,
		layerByAuth: layerByAuth,
		order:       order,
		total:       total,
	})
}

func removeAccountFromLayer(accounts []Account, authID string) []Account {
	remaining := make([]Account, 0, len(accounts))
	for _, account := range accounts {
		if account.ID != authID {
			remaining = append(remaining, account)
		}
	}
	return remaining
}

func insertAccountSorted(accounts []Account, account Account) []Account {
	position := sort.Search(len(accounts), func(index int) bool {
		return accounts[index].ID >= account.ID
	})
	if position < len(accounts) && accounts[position].ID == account.ID {
		updated := append([]Account(nil), accounts...)
		updated[position] = account
		return updated
	}
	updated := make([]Account, 0, len(accounts)+1)
	updated = append(updated, accounts[:position]...)
	updated = append(updated, account)
	updated = append(updated, accounts[position:]...)
	return updated
}

func buildSelectionOrder(layers map[string]routingSelectionLayer) ([]string, int) {
	ordered := make([]routingSelectionLayer, 0, len(layers))
	total := 0
	for _, layer := range layers {
		if len(layer.Accounts) == 0 {
			continue
		}
		ordered = append(ordered, layer)
		total += len(layer.Accounts)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].QualityRank != ordered[j].QualityRank {
			return ordered[i].QualityRank < ordered[j].QualityRank
		}
		return ordered[i].Priority > ordered[j].Priority
	})
	order := make([]string, 0, len(ordered))
	for _, layer := range ordered {
		order = append(order, layer.Key)
	}
	return order, total
}

func (p *Plugin) rebuildSelectionIndex() error {
	if p == nil {
		return nil
	}
	p.selectionIndex.buildMu.Lock()
	defer p.selectionIndex.buildMu.Unlock()
	store := p.stateStore()
	var states map[string]*AccountState
	if store != nil {
		states = store.Snapshot().Accounts
	}
	p.selectionIndex.Build(p.catalogSnapshot(), states)
	return nil
}

func (p *Plugin) ensureSelectionIndex() error {
	if p == nil || p.selectionIndex.Ready() {
		return nil
	}
	return p.rebuildSelectionIndex()
}

func (p *Plugin) refreshSelectionAccount(authID string) {
	if p == nil || authID == "" {
		return
	}
	index := &p.selectionIndex
	index.buildMu.Lock()
	defer index.buildMu.Unlock()
	if !index.Ready() {
		return
	}
	account, ok := p.findAccount(authID)
	if !ok {
		index.Remove(authID)
		return
	}
	state, okState := p.stateAccount(account.ID)
	if !okState && account.AuthIndex != "" {
		state, _ = p.stateAccount(account.AuthIndex)
	}
	index.Upsert(account, state)
}

func (runtime *routingSelectionRuntime) StrategyStart(layerKey, strategy string, count int) int {
	if count <= 0 {
		return 0
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	switch strategy {
	case "random":
		return rand.IntN(count)
	case "fill_first":
		return 0
	default:
		start := runtime.cursors[layerKey] % count
		runtime.cursors[layerKey] = (start + 1) % count
		return start
	}
}

func (runtime *routingSelectionRuntime) ReserveAttempt(authID string) {
	if authID == "" {
		return
	}
	runtime.mu.Lock()
	runtime.activeAttempts[authID]++
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) TryReserveIdle(authID string) bool {
	if authID == "" {
		return false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeAttempts[authID] > 0 {
		return false
	}
	runtime.activeAttempts[authID] = 1
	return true
}

func (runtime *routingSelectionRuntime) ReleaseAttempt(authID string) {
	if authID == "" {
		return
	}
	runtime.mu.Lock()
	if runtime.activeAttempts[authID] <= 1 {
		delete(runtime.activeAttempts, authID)
	} else {
		runtime.activeAttempts[authID]--
	}
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) TryReservePreferred(authID string) bool {
	return runtime.TryReserveIdle(authID)
}

func (runtime *routingSelectionRuntime) Preferred() (string, time.Time) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.preferredAuthID, runtime.preferredSuccessAt
}

func (runtime *routingSelectionRuntime) SetPreferred(authID string, now time.Time) {
	runtime.mu.Lock()
	runtime.preferredAuthID = authID
	runtime.preferredSuccessAt = now
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) RefreshPreferred(authID string, now time.Time) {
	runtime.mu.Lock()
	if runtime.preferredAuthID == authID {
		runtime.preferredSuccessAt = now
	}
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) ClearPreferred(authID string) {
	runtime.mu.Lock()
	if runtime.preferredAuthID == authID {
		runtime.preferredAuthID = ""
		runtime.preferredSuccessAt = time.Time{}
	}
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) LookupBinding(sessionKey string, now time.Time) (string, bool) {
	if sessionKey == "" {
		return "", false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	binding, ok := runtime.bindings[sessionKey]
	if !ok {
		return "", false
	}
	if !now.Before(binding.ExpiresAt) {
		delete(runtime.bindings, sessionKey)
		return "", false
	}
	return binding.AuthID, true
}

func (runtime *routingSelectionRuntime) GetOrBind(sessionKey, proposedAuthID string, now time.Time, ttl time.Duration, maxEntries int) string {
	if sessionKey == "" || proposedAuthID == "" {
		return proposedAuthID
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.pruneBindingsLocked(now)
	if binding, ok := runtime.bindings[sessionKey]; ok && now.Before(binding.ExpiresAt) {
		return binding.AuthID
	}
	if maxEntries > 0 && len(runtime.bindings) >= maxEntries {
		runtime.evictOldestBindingLocked()
	}
	runtime.bindings[sessionKey] = routingSessionBinding{
		AuthID:    proposedAuthID,
		ExpiresAt: now.Add(ttl),
		BoundAt:   now,
	}
	return proposedAuthID
}

func (runtime *routingSelectionRuntime) RefreshBinding(sessionKey, authID string, now time.Time, ttl time.Duration) {
	if sessionKey == "" || authID == "" {
		return
	}
	runtime.mu.Lock()
	runtime.bindings[sessionKey] = routingSessionBinding{
		AuthID:    authID,
		ExpiresAt: now.Add(ttl),
		BoundAt:   now,
	}
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) DeleteIfBoundTo(sessionKey, authID string) {
	if sessionKey == "" || authID == "" {
		return
	}
	runtime.mu.Lock()
	if binding, ok := runtime.bindings[sessionKey]; ok && binding.AuthID == authID {
		delete(runtime.bindings, sessionKey)
	}
	runtime.mu.Unlock()
}

func (runtime *routingSelectionRuntime) pruneBindingsLocked(now time.Time) {
	for key, binding := range runtime.bindings {
		if !now.Before(binding.ExpiresAt) {
			delete(runtime.bindings, key)
		}
	}
}

func (runtime *routingSelectionRuntime) evictOldestBindingLocked() {
	oldestKey := ""
	var oldest time.Time
	for key, binding := range runtime.bindings {
		if oldestKey == "" || binding.BoundAt.Before(oldest) {
			oldestKey = key
			oldest = binding.BoundAt
		}
	}
	if oldestKey != "" {
		delete(runtime.bindings, oldestKey)
	}
}
