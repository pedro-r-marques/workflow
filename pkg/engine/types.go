package engine

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pedro-r-marques/workflow/pkg/config"
)

type LogEntry struct {
	Step     string
	Start    time.Time
	Children []uuid.UUID
	End      time.Time
	Worker   string
	Data     []byte
}

type workflowNode struct {
	Name      string
	Queue     string
	Task      string
	Ancestors []string
}

type workflowNodeMap map[string][]*workflowNode
type workflowState struct {
	VHost          string
	Name           string
	Config         config.Workflow
	NodeSuccessors workflowNodeMap
	TaskSuccessors map[string]workflowNodeMap
	InProgress     []uuid.UUID
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
