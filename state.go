package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
)

type Metrics struct {
	FirstTokenMS    int64   `json:"first_token_ms"`
	DurationMS      int64   `json:"duration_ms"`
	GenerationMS    int64   `json:"generation_ms"`
	OutputTokens    int64   `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	VisibleTokens   int64   `json:"visible_tokens"`
	EncryptedBytes  int64   `json:"encrypted_bytes"`
	TPS             float64 `json:"tps"`
}

type ResinState struct {
	Managed    bool   `json:"managed"`
	Account    string `json:"account"`
	Generation int64  `json:"generation"`
}

type AccountState struct {
	AuthID                       string     `json:"auth_id"`
	Email                        string     `json:"email,omitempty"`
	Label                        string     `json:"label,omitempty"`
	State                        string     `json:"state"`
	LastReason                   string     `json:"last_reason,omitempty"`
	FirstSeenAt                  time.Time  `json:"first_seen_at,omitempty"`
	LastObservedAt               time.Time  `json:"last_observed_at,omitempty"`
	LastProbeAt                  time.Time  `json:"last_probe_at,omitempty"`
	CooldownUntil                time.Time  `json:"cooldown_until,omitempty"`
	QualityStrikeMissingThinking bool       `json:"quality_strike_missing_thinking"`
	ProbeBackoffUntil            time.Time  `json:"probe_backoff_until,omitempty"`
	DisableKind                  string     `json:"disable_kind,omitempty"`
	PreviousDisabled             bool       `json:"previous_disabled,omitempty"`
	Resin                        ResinState `json:"resin"`
	Metrics                      Metrics    `json:"metrics"`
}

type Event struct {
	Time   time.Time `json:"time"`
	AuthID string    `json:"auth_id,omitempty"`
	Class  string    `json:"class,omitempty"`
	Action string    `json:"action,omitempty"`
	Result string    `json:"result,omitempty"`
	Detail string    `json:"detail,omitempty"`
}

type State struct {
	SchemaVersion      int                      `json:"schema_version"`
	UpdatedAt          time.Time                `json:"updated_at"`
	Policy             Policy                   `json:"policy"`
	PolicyYAMLRevision string                   `json:"policy_yaml_revision,omitempty"`
	Resin              ResinSettings            `json:"resin,omitempty"`
	Accounts           map[string]*AccountState `json:"accounts"`
	Events             []Event                  `json:"events"`
}

type StateStore struct {
	mu    sync.RWMutex
	path  string
	state State
}

func NewStateStore(path string) *StateStore {
	if path == "" {
		path = defaultStatePath()
	}
	return &StateStore{path: path}
}

func (s *StateStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *StateStore) Load(defaultPolicy Policy, revision string) error {
	if s == nil {
		return errors.New("state store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, errRead := os.ReadFile(s.path)
	if errRead != nil {
		if !errors.Is(errRead, os.ErrNotExist) {
			return fmt.Errorf("read state file: %w", errRead)
		}
		s.state = State{
			SchemaVersion:      stateSchemaVersion,
			UpdatedAt:          time.Now().UTC(),
			Policy:             defaultPolicy,
			PolicyYAMLRevision: revision,
			Accounts:           make(map[string]*AccountState),
			Events:             []Event{},
		}
		return s.saveLocked()
	}
	var state State
	if errUnmarshal := json.Unmarshal(raw, &state); errUnmarshal != nil {
		return fmt.Errorf("decode state file: %w", errUnmarshal)
	}
	if state.SchemaVersion != stateSchemaVersion {
		return fmt.Errorf("unsupported state schema version %d", state.SchemaVersion)
	}
	if state.Accounts == nil {
		state.Accounts = make(map[string]*AccountState)
	}
	if len(state.Policy.ModelPrefixes) == 0 && state.Policy.EventsLimit == 0 {
		state.Policy = defaultPolicy
		state.PolicyYAMLRevision = revision
	} else if normalized, errNormalize := normalizePolicy(state.Policy); errNormalize != nil {
		return fmt.Errorf("state policy invalid: %w", errNormalize)
	} else {
		state.Policy = normalized
	}
	if state.PolicyYAMLRevision == "" {
		state.PolicyYAMLRevision = revision
	}
	for authID, account := range state.Accounts {
		if account == nil {
			delete(state.Accounts, authID)
			continue
		}
		if account.AuthID == "" {
			account.AuthID = authID
		}
		if account.State == "" {
			account.State = StateUnknown
		}
		if account.FirstSeenAt.IsZero() {
			account.FirstSeenAt = state.UpdatedAt
			if account.FirstSeenAt.IsZero() {
				account.FirstSeenAt = time.Now().UTC()
			}
		}
	}
	s.state = state
	return nil
}

func (s *StateStore) Snapshot() State {
	if s == nil {
		return State{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneState(s.state)
}

func (s *StateStore) Update(fn func(*State) error) error {
	if s == nil {
		return errors.New("state store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneState(s.state)
	if errFn := fn(&s.state); errFn != nil {
		s.state = previous
		return errFn
	}
	s.state.UpdatedAt = time.Now().UTC()
	if errSave := s.saveLocked(); errSave != nil {
		s.state = previous
		return errSave
	}
	return nil
}

func (s *StateStore) UpdateIfChanged(fn func(*State) error) error {
	if s == nil {
		return errors.New("state store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneState(s.state)
	if errFn := fn(&s.state); errFn != nil {
		s.state = previous
		return errFn
	}
	if reflect.DeepEqual(previous, s.state) {
		return nil
	}
	s.state.UpdatedAt = time.Now().UTC()
	if errSave := s.saveLocked(); errSave != nil {
		s.state = previous
		return errSave
	}
	return nil
}

func (s *StateStore) saveLocked() error {
	if s.path == "" {
		return errors.New("state path is empty")
	}
	if s.state.SchemaVersion == 0 {
		s.state.SchemaVersion = stateSchemaVersion
	}
	if s.state.Accounts == nil {
		s.state.Accounts = make(map[string]*AccountState)
	}
	if s.state.Events == nil {
		s.state.Events = []Event{}
	}
	trimEvents(&s.state, s.state.Policy.EventsLimit)
	raw, errMarshal := json.MarshalIndent(s.state, "", "  ")
	if errMarshal != nil {
		return fmt.Errorf("encode state: %w", errMarshal)
	}
	dir := filepath.Dir(s.path)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return fmt.Errorf("create state directory: %w", errMkdir)
	}
	_ = os.Chmod(dir, 0o700)
	temp, errTemp := os.CreateTemp(dir, ".state-*.tmp")
	if errTemp != nil {
		return fmt.Errorf("create state temp file: %w", errTemp)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if errChmod := temp.Chmod(0o600); errChmod != nil {
		_ = temp.Close()
		return fmt.Errorf("set state file permissions: %w", errChmod)
	}
	if _, errWrite := temp.Write(raw); errWrite != nil {
		_ = temp.Close()
		return fmt.Errorf("write state temp file: %w", errWrite)
	}
	if errSync := temp.Sync(); errSync != nil {
		_ = temp.Close()
		return fmt.Errorf("sync state temp file: %w", errSync)
	}
	if errClose := temp.Close(); errClose != nil {
		return fmt.Errorf("close state temp file: %w", errClose)
	}
	if errRename := os.Rename(tempName, s.path); errRename != nil {
		if errRemove := os.Remove(s.path); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return fmt.Errorf("replace state file: %w", errRename)
		}
		if errRetry := os.Rename(tempName, s.path); errRetry != nil {
			return fmt.Errorf("replace state file: %w", errRetry)
		}
	}
	return nil
}

func cloneState(state State) State {
	clone := state
	if state.Policy.ModelPrefixes != nil {
		clone.Policy.ModelPrefixes = append([]string{}, state.Policy.ModelPrefixes...)
	}
	clone.Accounts = make(map[string]*AccountState, len(state.Accounts))
	for key, account := range state.Accounts {
		if account == nil {
			continue
		}
		copied := *account
		clone.Accounts[key] = &copied
	}
	if state.Events != nil {
		clone.Events = append([]Event{}, state.Events...)
	}
	return clone
}

func trimEvents(state *State, limit int) {
	if state == nil {
		return
	}
	if limit <= 0 {
		limit = defaultEventsLimit
	}
	if len(state.Events) <= limit {
		return
	}
	state.Events = append([]Event(nil), state.Events[len(state.Events)-limit:]...)
}

func appendEvent(state *State, ev Event) {
	if state == nil {
		return
	}
	ev.Time = ev.Time.UTC()
	state.Events = append(state.Events, ev)
	trimEvents(state, state.Policy.EventsLimit)
}
