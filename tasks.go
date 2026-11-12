package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	taskStatusQueued    = "queued"
	taskStatusRunning   = "running"
	taskStatusSucceeded = "succeeded"
	taskStatusFailed    = "failed"

	taskRetention = time.Hour
	taskLimit     = 200
)

// Task is the management-facing view of one long-running operation.
type Task struct {
	ID          string    `json:"task_id"`
	Kind        string    `json:"kind"`
	AuthIDs     []string  `json:"auth_ids,omitempty"`
	Status      string    `json:"status"`
	Progress    string    `json:"progress,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	EndedAt     time.Time `json:"ended_at,omitempty"`
	Result      any       `json:"result,omitempty"`
	Error       string    `json:"error,omitempty"`
	ErrorStatus int       `json:"error_status,omitempty"`
}

type taskRecord struct {
	task   Task
	cancel context.CancelFunc
}

var taskSequence atomic.Uint64

func (p *Plugin) startTask(kind string, authIDs []string, progress string, fn func(context.Context) (any, error)) Task {
	if p == nil {
		return Task{Kind: kind, Status: taskStatusFailed, Error: "plugin is not initialized"}
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	task := Task{
		ID:        nextTaskID(now),
		Kind:      strings.TrimSpace(kind),
		AuthIDs:   cleanTaskAuthIDs(authIDs),
		Status:    taskStatusQueued,
		Progress:  strings.TrimSpace(progress),
		CreatedAt: now,
	}
	p.tasksMu.Lock()
	if p.tasks == nil {
		p.tasks = make(map[string]*taskRecord)
	}
	p.pruneTasksLocked(now)
	p.tasks[task.ID] = &taskRecord{task: task, cancel: cancel}
	p.taskWG.Add(1)
	p.tasksMu.Unlock()

	go func() {
		defer p.taskWG.Done()
		p.runTask(ctx, task.ID, fn)
	}()
	return cloneTask(task)
}

func nextTaskID(now time.Time) string {
	return fmt.Sprintf("%d-%d", now.UnixNano(), taskSequence.Add(1))
}

func cleanTaskAuthIDs(authIDs []string) []string {
	seen := make(map[string]struct{}, len(authIDs))
	out := make([]string, 0, len(authIDs))
	for _, authID := range authIDs {
		authID = strings.TrimSpace(authID)
		if authID == "" {
			continue
		}
		if _, exists := seen[authID]; exists {
			continue
		}
		seen[authID] = struct{}{}
		out = append(out, authID)
	}
	return out
}

func (p *Plugin) runTask(ctx context.Context, taskID string, fn func(context.Context) (any, error)) {
	p.tasksMu.Lock()
	record := p.tasks[taskID]
	if record == nil {
		p.tasksMu.Unlock()
		return
	}
	record.task.Status = taskStatusRunning
	record.task.StartedAt = time.Now().UTC()
	p.tasksMu.Unlock()

	var result any
	var errTask error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				errTask = fmt.Errorf("task panic: %v", recovered)
			}
		}()
		result, errTask = fn(ctx)
	}()

	p.tasksMu.Lock()
	record = p.tasks[taskID]
	if record != nil {
		record.task.EndedAt = time.Now().UTC()
		if errTask != nil {
			record.task.Status = taskStatusFailed
			record.task.Error = sanitizeError(errTask)
			record.task.ErrorStatus = taskErrorStatus(errTask)
			record.task.Result = nil
		} else {
			record.task.Status = taskStatusSucceeded
			record.task.Result = result
			record.task.Error = ""
		}
		record.task.Progress = ""
		record.cancel()
	}
	p.tasksMu.Unlock()
}

func taskErrorStatus(errValue error) int {
	var resinErr *resinConfigError
	if errors.As(errValue, &resinErr) {
		return resinErr.Status
	}
	return 0
}

func (p *Plugin) hasActiveTask(kind, authID string) bool {
	if p == nil {
		return false
	}
	kind = strings.TrimSpace(kind)
	authID = strings.TrimSpace(authID)
	p.tasksMu.Lock()
	defer p.tasksMu.Unlock()
	for _, record := range p.tasks {
		if record == nil || record.task.Kind != kind {
			continue
		}
		if record.task.Status != taskStatusQueued && record.task.Status != taskStatusRunning {
			continue
		}
		if authID == "" {
			return true
		}
		for _, candidate := range record.task.AuthIDs {
			if candidate == authID {
				return true
			}
		}
	}
	return false
}

func (p *Plugin) taskByID(taskID string) (Task, bool) {
	p.tasksMu.Lock()
	defer p.tasksMu.Unlock()
	record := p.tasks[strings.TrimSpace(taskID)]
	if record == nil {
		return Task{}, false
	}
	return cloneTask(record.task), true
}

func (p *Plugin) listTasks(limit int) []Task {
	if limit <= 0 {
		limit = 50
	}
	if limit > taskLimit {
		limit = taskLimit
	}
	p.tasksMu.Lock()
	defer p.tasksMu.Unlock()
	p.pruneTasksLocked(time.Now().UTC())
	tasks := make([]Task, 0, len(p.tasks))
	for _, record := range p.tasks {
		if record != nil {
			tasks = append(tasks, cloneTask(record.task))
		}
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAt.After(tasks[j].CreatedAt) })
	if len(tasks) > limit {
		tasks = tasks[:limit]
	}
	return tasks
}

func (p *Plugin) pruneTasksLocked(now time.Time) {
	for id, record := range p.tasks {
		if record == nil {
			delete(p.tasks, id)
			continue
		}
		finished := record.task.Status == taskStatusSucceeded || record.task.Status == taskStatusFailed
		if finished && !record.task.EndedAt.IsZero() && now.Sub(record.task.EndedAt) > taskRetention {
			delete(p.tasks, id)
		}
	}
	if len(p.tasks) <= taskLimit {
		return
	}
	ids := make([]string, 0, len(p.tasks))
	for id := range p.tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := p.tasks[ids[i]]
		right := p.tasks[ids[j]]
		if left == nil {
			return true
		}
		if right == nil {
			return false
		}
		return left.task.CreatedAt.Before(right.task.CreatedAt)
	})
	for _, id := range ids[:len(ids)-taskLimit] {
		if record := p.tasks[id]; record != nil && record.cancel != nil {
			record.cancel()
		}
		delete(p.tasks, id)
	}
}

func (p *Plugin) cancelTasks() {
	p.tasksMu.Lock()
	defer p.tasksMu.Unlock()
	for _, record := range p.tasks {
		if record != nil && record.cancel != nil {
			record.cancel()
		}
	}
}

func cloneTask(task Task) Task {
	cloned := task
	if task.AuthIDs != nil {
		cloned.AuthIDs = append([]string(nil), task.AuthIDs...)
	}
	return cloned
}
