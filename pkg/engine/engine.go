package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pedro-r-marques/workflow/pkg/config"
)

type WorkflowEngine interface {
	// Workflow configuration
	Update(*config.Workflow) error
	Delete(name string) error
	ListWorkflows() []string

	// Workflow item
	Create(workflow string, id uuid.UUID, dict map[string]json.RawMessage) error
	Cancel(id uuid.UUID) error
	OnEvent(msg map[string]json.RawMessage) error
	Watch(id uuid.UUID, allEvents bool, ch chan LogEntry) error
	ListItems(workflow string) ([]uuid.UUID, error)
}

type engine struct {
	mbus      MessageBus
	store     ItemStore
	workflows map[string]*workflowState
	items     map[uuid.UUID]*workItem
	mutex     sync.Mutex
}

func NewWorkflowEngine(mbus MessageBus, store ItemStore) WorkflowEngine {
	return &engine{
		mbus:      mbus,
		store:     store,
		workflows: make(map[string]*workflowState),
		items:     make(map[uuid.UUID]*workItem),
	}
}

func newWorkflowNode(name string, step *config.WorkflowStep) *workflowNode {
	queue := step.Queue
	if queue == "" {
		queue = step.Name
	}
	node := &workflowNode{
		Name:  name,
		Queue: queue,
		Task:  step.Task,
	}
	return node
}

func addSuccessors(nodes map[string]*workflowNode, successors workflowNodeMap, prefix []string, steps []*config.WorkflowStep, parent *workflowNode) {
	for _, step := range steps {
		path := append(prefix, step.Name)
		stepName := strings.Join(path, "/")
		node, exists := nodes[stepName]
		if !exists {
			node = newWorkflowNode(stepName, step)
			nodes[stepName] = node
		}

		addPrefix := true
		depends := step.Depends
		if parent != nil && reflect.DeepEqual(depends, []string{config.WorkflowStart}) {
			depends = parent.Ancestors
			addPrefix = false
		}
		for _, dep := range depends {
			depPath := dep
			if addPrefix {
				depPath = strings.Join(append(prefix, dep), "/")
			}
			if depNode, ok := nodes[depPath]; ok && depNode.Task != "" {
				depPath = depPath + "/" + config.WorkflowEnd
			}

			nlist, exists := successors[depPath]
			if !exists {
				nlist = make([]*workflowNode, 0, 1)
			}

			if step.Task == "" {
				successors[depPath] = append(nlist, node)
			}
			node.Ancestors = append(node.Ancestors, depPath)
		}
	}
}

// short-cut task end nodes from the graph.
func successorsXCutTaskEnd(wconfig *config.Workflow, successors workflowNodeMap) {
	var nodesWithEndAncestors []*workflowNode
	for k, succ := range successors {
		var modified bool
		nlist := make([]*workflowNode, 0, len(succ))
		for _, node := range succ {
			elements := strings.Split(node.Name, "/")
			if len(elements) > 1 && elements[len(elements)-1] == config.WorkflowEnd {
				next := successors[node.Name]
				for _, x := range next {
					x.Ancestors = append(x.Ancestors, k)
					nodesWithEndAncestors = append(nodesWithEndAncestors, x)
				}
				nlist = append(nlist, next...)
				modified = true
			} else {
				nlist = append(nlist, node)
			}
		}
		if modified {
			successors[k] = nlist
		}
	}
	for _, node := range nodesWithEndAncestors {
		for i := len(node.Ancestors) - 1; i >= 0; i-- {
			a := node.Ancestors[i]
			if strings.HasSuffix(a, "/"+config.WorkflowEnd) {
				node.Ancestors = append(node.Ancestors[:i], node.Ancestors[i+1:]...)
			}
		}
	}
	for _, step := range wconfig.Steps {
		if step.Task == "" {
			continue
		}
		delete(successors, step.Name+"/"+config.WorkflowEnd)
	}
}

func makeSuccessors(wconfig *config.Workflow) workflowNodeMap {
	nodes := make(map[string]*workflowNode)
	successors := make(workflowNodeMap)
	addSuccessors(nodes, successors, []string{}, wconfig.Steps, nil)

	var hasTasks bool
	for _, step := range wconfig.Steps {
		if step.Task == "" {
			continue
		}
		hasTasks = true
		for _, task := range wconfig.Tasks {
			if task.Name == step.Task {
				path := []string{step.Name}
				addSuccessors(nodes, successors, path, task.Steps, nodes[step.Name])
				break
			}
		}
	}

	if hasTasks {
		successorsXCutTaskEnd(wconfig, successors)
	}
	return successors
}

func (e *engine) Update(config *config.Workflow) error {
	// TODO: garbage collect previous vhosts.
	var workflowNames []string
	if len(config.VHosts) > 0 {
		workflowNames = make([]string, len(config.VHosts))
		for i, vhost := range config.VHosts {
			workflowNames[i] = strings.Join([]string{vhost, config.Name}, "/")
		}
	} else {
		workflowNames = []string{config.Name}
	}
	for i, name := range workflowNames {
		wstate := &workflowState{
			Workflow:       *config,
			NodeSuccessors: makeSuccessors(config),
		}
		if len(config.VHosts) > 0 {
			wstate.VHost = config.VHosts[i]
		}
		e.workflows[name] = wstate
	}
	return nil
}

func (e *engine) Delete(name string) error { return nil }
func (e *engine) ListWorkflows() []string  { return nil }

func makeMessage(id uuid.UUID, nodeName string, data map[string]json.RawMessage) map[string]json.RawMessage {
	msg := make(map[string]json.RawMessage, len(data)+2)
	for k, v := range data {
		msg[k] = v
	}
	msg["id"] = json.RawMessage(id.String())
	msg["node"] = json.RawMessage(nodeName)
	return msg
}

func (e *engine) workflowStart(item *workItem, data map[string]json.RawMessage) error {
	wrk := item.Workflow
	succ, ok := wrk.NodeSuccessors[config.WorkflowStart]
	if !ok {
		return fmt.Errorf("invalid workflow: %s", wrk.Workflow.Name)
	}

	item.mutex.Lock()
	defer item.mutex.Unlock()

	for _, node := range succ {
		log := &LogEntry{
			Step:  node.Name,
			Start: time.Now(),
		}
		item.Open = append(item.Open, log)
		msg := makeMessage(item.ID, node.Name, data)
		e.mbus.SendMsg(wrk.VHost, node.Queue, msg)
	}
	return nil
}

func (e *engine) Create(workflow string, id uuid.UUID, dict map[string]json.RawMessage) error {
	if _, exists := e.items[id]; exists {
		return fmt.Errorf("duplicate item id: %v", id)
	}
	wstate, exists := e.workflows[workflow]
	if !exists {
		return fmt.Errorf("unknown workflow: %s", workflow)
	}

	e.mutex.Lock()
	e.items[id] = &workItem{
		ID:       id,
		Workflow: wstate,
		Closed:   make(map[string]*LogEntry),
	}
	e.mutex.Unlock()
	return e.workflowStart(e.items[id], dict)
}

func dependencyCheck(item *workItem, node *workflowNode) bool {
	for _, dep := range node.Ancestors {
		if _, ok := item.Closed[dep]; !ok {
			return false
		}
	}
	return true
}

// called with mutex locked
func (e *engine) advanceState(item *workItem, stepName string, data map[string]json.RawMessage, changes []*LogEntry) error {
	wrk := item.Workflow
	succ, ok := wrk.NodeSuccessors[stepName]
	if !ok {
		return fmt.Errorf("internal error: %s no successors for node %s", wrk.Name, stepName)
	}

	var done bool
	for _, node := range succ {
		if !dependencyCheck(item, node) {
			continue
		}

		if node.Name == config.WorkflowEnd {
			done = true
			break
		}

		log := &LogEntry{
			Step:  node.Name,
			Start: time.Now(),
		}
		item.Open = append(item.Open, log)
		msg := makeMessage(item.ID, node.Name, data)
		e.mbus.SendMsg(wrk.VHost, node.Queue, msg)
	}

	if done {
		e.completed(item, changes)
	}
	return nil
}

func (e *engine) completed(item *workItem, changes []*LogEntry) {
	log := LogEntry{
		Step:  config.WorkflowEnd,
		Start: time.Now(),
	}

	_ = append(changes, &log)
	if item.watcher != nil {
		item.watcher <- log
		close(item.watcher)
	}
}

func (e *engine) OnEvent(msg map[string]json.RawMessage) error {
	idStr, exists := msg["id"]
	if !exists {
		return fmt.Errorf("invalid message: mandatory field \"id\" missing")
	}
	node, exists := msg["node"]
	if !exists {
		return fmt.Errorf("invalid message: mandatory field \"node\" missing")
	}

	id, err := uuid.Parse(string(idStr))
	if err != nil {
		return fmt.Errorf("invalid message id: %w", err)
	}

	e.mutex.Lock()
	item, exists := e.items[id]
	e.mutex.Unlock()
	if !exists {
		return fmt.Errorf("unknown item id: %v", id)
	}

	item.mutex.Lock()
	defer item.mutex.Unlock()

	var logEntry *LogEntry
	for i, entry := range item.Open {
		if entry.Step == string(node) {
			logEntry = entry
			item.Open = append(item.Open[:i], item.Open[i+1:]...)
			break
		}
	}
	if logEntry == nil {
		return fmt.Errorf("unexpected message for id %v from %s", id, node)
	}
	logEntry.End = time.Now()
	item.Closed[logEntry.Step] = logEntry

	if item.watcher != nil && item.watchAll {
		item.watcher <- *logEntry
	}

	changes := make([]*LogEntry, 1, 2)
	changes[0] = logEntry

	if err := e.advanceState(item, logEntry.Step, msg, changes); err != nil {
		return err
	}

	if err := e.store.Update(item.ID, item.Workflow.Name, changes); err != nil {
		log.Print(err)
	}

	return nil
}

func (e *engine) Watch(id uuid.UUID, allEvents bool, ch chan LogEntry) error {
	e.mutex.Lock()
	item, exists := e.items[id]
	e.mutex.Unlock()
	if !exists {
		return fmt.Errorf("unknown item id %v", id)
	}

	item.mutex.Lock()
	defer item.mutex.Unlock()

	item.watcher = ch
	item.watchAll = allEvents
	return nil
}

func (e *engine) Cancel(id uuid.UUID) error                      { return nil }
func (e *engine) ListItems(workflow string) ([]uuid.UUID, error) { return nil, nil }
