package engine

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
)

type LogEntry struct {
	Step     string
	Start    time.Time
	Children []uuid.UUID
	End      time.Time
	Worker   string
	Data     []byte
}

type JobStatusEntry struct {
	Name     string          `json:"name"`
	Start    time.Time       `json:"startTime"`
	Elapsed  time.Duration   `json:"elapsed,omitempty"`
	Children []uuid.UUID     `json:"tasks,omitempty"`
	Worker   string          `json:"worker,omitempty"`
	Data     json.RawMessage `json:"message,omitempty"`
}

type jobState struct {
	ID       uuid.UUID
	Workflow *workflowState
	mutex    sync.Mutex
	Open     []*LogEntry
	Closed   map[string]*LogEntry
	watcher  chan LogEntry
	watchAll bool
	Task     string
	Parent   *jobState
}
