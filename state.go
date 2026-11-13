package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

type QualityEvidence struct {
	At                time.Time `json:"at,omitempty"`
	Source            string    `json:"source,omitempty"`
	Action            string    `json:"action,omitempty"`
	Class             string    `json:"class,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	Protocol          string    `json:"protocol,omitempty"`
	Streaming         bool      `json:"streaming"`
	Terminal          bool      `json:"terminal"`
	HoldExpired       bool      `json:"hold_expired"`
	HasThinking       bool      `json:"has_thinking"`
	HasReasoningDelta bool      `json:"has_reasoning_delta"`
	ReasoningStarted  bool      `json:"reasoning_started"`
	SemanticOutput    bool      `json:"semantic_output"`
	UsageReported     bool      `json:"usage_reported"`
	VisibleRunes      int64     `json:"visible_runes"`
	VisibleTokens     int64     `json:"visible_tokens"`
	OutputTokens      int64     `json:"output_tokens"`
	ReasoningTokens   int64     `json:"reasoning_tokens"`
	EncryptedBytes    int64     `json:"encrypted_bytes"`
	EncryptedFloor    int64     `json:"encrypted_floor"`
	FirstTokenMS      int64     `json:"first_token_ms"`
	DurationMS        int64     `json:"duration_ms"`
	GenerationMS      int64     `json:"generation_ms"`
	VisibleFlushMS    int64     `json:"visible_flush_ms"`
	TPS               float64   `json:"tps"`
	ChunkCount        int       `json:"chunk_count"`
	ByteCount         int64     `json:"byte_count"`
	EventCount        int       `json:"event_count"`
	ParsedEvents      int       `json:"parsed_events"`
	UnknownEvents     int       `json:"unknown_events"`
	ParseFailures     int       `json:"parse_failures"`
}

func qualityEvidenceFromVerdict(verdict QualityVerdict, source string, at time.Time) *QualityEvidence {
	sig := verdict.Signals
	return &QualityEvidence{
		At: at, Source: source, Action: verdict.Action, Class: verdict.Class, Reason: verdict.Reason,
		Protocol: sig.Protocol, Streaming: sig.Streaming, Terminal: sig.Terminal, HoldExpired: sig.HoldExpired,
		HasThinking: sig.HasThinking, HasReasoningDelta: sig.HasReasoningDelta, ReasoningStarted: sig.ReasoningStarted,
		SemanticOutput: sig.SemanticOutput, UsageReported: sig.UsageReported,
		VisibleRunes: sig.VisibleRunes, VisibleTokens: sig.VisibleTokens, OutputTokens: sig.OutputTokens,
		ReasoningTokens: sig.ReasoningTokens, EncryptedBytes: sig.EncryptedBytes, EncryptedFloor: sig.EncryptedFloor,
		FirstTokenMS: sig.FirstTokenMS, DurationMS: sig.DurationMS, GenerationMS: sig.GenerationMS,
		VisibleFlushMS: sig.VisibleFlushMS, TPS: sig.TPS,
		ChunkCount: sig.ChunkCount, ByteCount: sig.ByteCount, EventCount: sig.EventCount,
		ParsedEvents: sig.ParsedEvents, UnknownEvents: sig.UnknownEvents, ParseFailures: sig.ParseFailures,
	}
}

type AttemptEvidence struct {
	At     time.Time `json:"at,omitempty"`
	Source string    `json:"source,omitempty"`
	Class  string    `json:"class,omitempty"`
	Reason string    `json:"reason,omitempty"`
}
type ResinState struct {
	Managed    bool   `json:"managed"`
	Account    string `json:"account"`
	Generation int64  `json:"generation"`
}

type ResinLeaseDelete struct {
	AuthID        string    `json:"auth_id,omitempty"`
	AdminURL      string    `json:"admin_url,omitempty"`
	AdminToken    string    `json:"admin_token,omitempty"`
	PlatformID    string    `json:"platform_id,omitempty"`
	Account       string    `json:"account"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	Attempts      int       `json:"attempts,omitempty"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
}

type AccountState struct {
	AuthID                       string           `json:"auth_id"`
	Email                        string           `json:"email,omitempty"`
	Label                        string           `json:"label,omitempty"`
	State                        string           `json:"state"`
	LastReason                   string           `json:"last_reason,omitempty"`
	LastAttempt                  *AttemptEvidence `json:"last_attempt,omitempty"`
	LastEvidence                 *QualityEvidence `json:"last_evidence,omitempty"`
	FirstSeenAt                  time.Time        `json:"first_seen_at,omitempty"`
	LastObservedAt               time.Time        `json:"last_observed_at,omitempty"`
	LastProbeAt                  time.Time        `json:"last_probe_at,omitempty"`
	CooldownUntil                time.Time        `json:"cooldown_until,omitempty"`
	QualityStrikeMissingThinking bool             `json:"quality_strike_missing_thinking"`
	SoftStrikes                  int              `json:"soft_strikes,omitempty"`
	ErrorStrikes                 int              `json:"error_strikes,omitempty"`
	ThinkingStrikes              int              `json:"thinking_strikes,omitempty"`
	ProbeBackoffUntil            time.Time        `json:"probe_backoff_until,omitempty"`
	DisableKind                  string           `json:"disable_kind,omitempty"`
	PreviousDisabled             bool             `json:"previous_disabled,omitempty"`
	AuthDisabledIntent           *bool            `json:"auth_disabled_intent,omitempty"`
	Resin                        ResinState       `json:"resin"`
	Metrics                      Metrics          `json:"metrics"`
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
	ResinLeaseDeletes  []ResinLeaseDelete       `json:"resin_lease_deletes,omitempty"`
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.path
}

// SetPath moves the live state to a new file without losing concurrent updates.
// It refuses to overwrite an existing file unless the caller explicitly restores
// a previously active path after a failed pointer update.
func (s *StateStore) SetPath(path string) error {
	return s.setPath(path, false)
}

func (s *StateStore) setPath(path string, overwrite bool) error {
	if s == nil {
		return errors.New("state store is nil")
	}
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return fmt.Errorf("%w: state path is empty", os.ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if path == s.path {
		return nil
	}
	if info, errStat := os.Stat(path); errStat == nil {
		if info.IsDir() {
			return fmt.Errorf("%w: state path is a directory", os.ErrInvalid)
		}
		if !overwrite {
			return fmt.Errorf("%w: state file already exists: %s", os.ErrExist, path)
		}
	} else if !errors.Is(errStat, os.ErrNotExist) {
		return fmt.Errorf("inspect state file: %w", errStat)
	}
	oldPath := s.path
	oldUpdatedAt := s.state.UpdatedAt
	s.path = path
	s.state.UpdatedAt = time.Now().UTC()
	if errSave := s.saveLocked(); errSave != nil {
		s.path = oldPath
		s.state.UpdatedAt = oldUpdatedAt
		return errSave
	}
	return nil
}

func configuredStatePath(value string) string {
	if path := strings.TrimSpace(value); path != "" {
		return filepath.Clean(path)
	}
	return defaultStatePath()
}

func normalizeStatePath(value string) (string, error) {
	path := strings.TrimSpace(value)
	if path == "" {
		return "", fmt.Errorf("%w: state_file is required", os.ErrInvalid)
	}
	absolute, errAbsolute := filepath.Abs(path)
	if errAbsolute != nil {
		return "", fmt.Errorf("%w: invalid state_file: %v", os.ErrInvalid, errAbsolute)
	}
	absolute = filepath.Clean(absolute)
	if info, errStat := os.Stat(absolute); errStat == nil && info.IsDir() {
		return "", fmt.Errorf("%w: state_file must point to a file", os.ErrInvalid)
	} else if errStat != nil && !errors.Is(errStat, os.ErrNotExist) {
		return "", fmt.Errorf("inspect state_file: %w", errStat)
	}
	return absolute, nil
}

func statePathPointerPath(bootstrapPath string) string {
	return filepath.Clean(bootstrapPath) + ".location"
}

func readStatePathPointer(bootstrapPath string) (string, error) {
	pointerPath := statePathPointerPath(bootstrapPath)
	raw, errRead := os.ReadFile(pointerPath)
	if errRead != nil {
		if errors.Is(errRead, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read state path pointer: %w", errRead)
	}
	return strings.TrimSpace(string(raw)), nil
}

func writeStatePathPointer(bootstrapPath, activePath string) error {
	pointerPath := statePathPointerPath(bootstrapPath)
	if filepath.Clean(bootstrapPath) == filepath.Clean(activePath) {
		if errRemove := os.Remove(pointerPath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return fmt.Errorf("remove state path pointer: %w", errRemove)
		}
		return nil
	}
	dir := filepath.Dir(pointerPath)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return fmt.Errorf("create state path pointer directory: %w", errMkdir)
	}
	temp, errTemp := os.CreateTemp(dir, ".state-location-*.tmp")
	if errTemp != nil {
		return fmt.Errorf("create state path pointer temp file: %w", errTemp)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, errWrite := temp.WriteString(filepath.Clean(activePath) + "\n"); errWrite != nil {
		_ = temp.Close()
		return fmt.Errorf("write state path pointer: %w", errWrite)
	}
	if errSync := temp.Sync(); errSync != nil {
		_ = temp.Close()
		return fmt.Errorf("sync state path pointer: %w", errSync)
	}
	if errClose := temp.Close(); errClose != nil {
		return fmt.Errorf("close state path pointer: %w", errClose)
	}
	if errRename := os.Rename(tempName, pointerPath); errRename != nil {
		if errRemove := os.Remove(pointerPath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return fmt.Errorf("replace state path pointer: %w", errRename)
		}
		if errRetry := os.Rename(tempName, pointerPath); errRetry != nil {
			return fmt.Errorf("replace state path pointer: %w", errRetry)
		}
	}
	return nil
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

func (s *StateStore) AccountSnapshot(authID string) (AccountState, bool) {
	if s == nil {
		return AccountState{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	account, ok := s.state.Accounts[authID]
	if !ok || account == nil {
		return AccountState{}, false
	}
	return *cloneAccountState(account), true
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
		clone.Accounts[key] = cloneAccountState(account)
	}
	if state.Events != nil {
		clone.Events = append([]Event{}, state.Events...)
	}
	if state.ResinLeaseDeletes != nil {
		clone.ResinLeaseDeletes = append([]ResinLeaseDelete{}, state.ResinLeaseDeletes...)
	}
	return clone
}

func cloneAccountState(account *AccountState) *AccountState {
	if account == nil {
		return nil
	}
	copied := *account
	if account.LastAttempt != nil {
		attempt := *account.LastAttempt
		copied.LastAttempt = &attempt
	}
	if account.LastEvidence != nil {
		evidence := *account.LastEvidence
		copied.LastEvidence = &evidence
	}
	if account.AuthDisabledIntent != nil {
		intent := *account.AuthDisabledIntent
		copied.AuthDisabledIntent = &intent
	}
	return &copied
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
