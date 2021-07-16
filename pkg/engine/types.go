package engine

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pedro-r-marques/workflow/pkg/config"
)

type LogEntry struct {
	Step   string
	Start  time.Time
	End    time.Time
	Worker string
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
	Workflow       config.Workflow
	NodeSuccessors workflowNodeMap
	InProgress     []uuid.UUID
}

type workItem struct {
	ID       uuid.UUID
	Workflow *workflowState
	mutex    sync.Mutex
	Open     []*LogEntry
	Closed   map[string]*LogEntry
	watcher  chan LogEntry
	watchAll bool
}
