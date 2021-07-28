package engine

import (
	"sync"

	"github.com/google/uuid"
	"github.com/pedro-r-marques/workflow/pkg/config"
)

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
	mutex          sync.Mutex
	jobs           []uuid.UUID
	taskStates     map[string]*workflowState
}

func (w *workflowState) locateTaskState(nodeName, taskName string) *workflowState {
	key := w.Name + "/" + nodeName
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if wstate, exists := w.taskStates[key]; exists {
		return wstate
	}
	wstate := &workflowState{
		VHost:          w.VHost,
		Name:           w.Name + "/" + nodeName,
		Config:         w.Config,
		NodeSuccessors: w.TaskSuccessors[taskName],
		TaskSuccessors: w.TaskSuccessors,
		taskStates:     make(map[string]*workflowState),
	}
	w.taskStates[key] = wstate
	return wstate
}

func (w *workflowState) onJobCreate(job *jobState) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.jobs = append(w.jobs, job.ID)
}

func (w *workflowState) onJobDone(job *jobState) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for ix, id := range w.jobs {
		if id == job.ID {
			if ix != len(w.jobs)-1 {
				w.jobs[ix] = w.jobs[len(w.jobs)-1]
			}
			w.jobs = w.jobs[:len(w.jobs)-1]
		}
	}
}

func addSuccessors(successors workflowNodeMap, steps []*config.WorkflowStep) {
	nodes := make(map[string]*workflowNode)

	for _, step := range steps {
		node, exists := nodes[step.Name]
		if !exists {
			node = newWorkflowNode(step.Name, step)
			nodes[step.Name] = node
		}

		for _, dep := range step.Depends {
			nlist, exists := successors[dep]
			if !exists {
				nlist = make([]*workflowNode, 0, 1)
			}

			successors[dep] = append(nlist, node)
			node.Ancestors = append(node.Ancestors, dep)
		}
	}
}

func makeSuccessors(wconfig *config.Workflow) workflowNodeMap {
	successors := make(workflowNodeMap)
	addSuccessors(successors, wconfig.Steps)
	return successors
}

func makeTaskSuccessors(wconfig *config.Workflow) map[string]workflowNodeMap {
	if len(wconfig.Tasks) == 0 {
		return nil
	}
	taskSuccessors := make(map[string]workflowNodeMap, len(wconfig.Tasks))
	for _, task := range wconfig.Tasks {
		successors := make(workflowNodeMap)
		addSuccessors(successors, task.Steps)
		taskSuccessors[task.Name] = successors
	}

	return taskSuccessors
}

func makeWorkflowState(wconfig *config.Workflow) *workflowState {
	wstate := &workflowState{
		Name:           wconfig.Name,
		Config:         *wconfig,
		NodeSuccessors: makeSuccessors(wconfig),
		TaskSuccessors: makeTaskSuccessors(wconfig),
		taskStates:     make(map[string]*workflowState),
	}
	return wstate
}
