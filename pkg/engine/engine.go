package engine

//go:generate mockgen -source engine.go -destination ./mock/engine.go

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pedro-r-marques/workflow/pkg/config"
)

const correlationIdSepToken = ":"

var errTaskNop = errors.New("no elements for task")

type WorkflowEngine interface {
	// Workflow configuration
	Update(*config.Workflow) error
	Delete(name string) error
	ListWorkflows() []string

	// Workflow item
	Create(workflow string, id uuid.UUID, dict map[string]json.RawMessage) error
	Cancel(id uuid.UUID) error
	OnEvent(correlationId string, body []byte) error
	Watch(id uuid.UUID, allEvents bool, ch chan LogEntry) error
	ListWorkflowJobs(workflow string) ([]uuid.UUID, error)
	ListJobs() []uuid.UUID
}

type engine struct {
	mbus      MessageBus
	store     ItemStore
	workflows map[string]*workflowState
	jobs      map[uuid.UUID]*jobState
	mutex     sync.Mutex
}

func NewWorkflowEngine(mbus MessageBus, store ItemStore) WorkflowEngine {
	return &engine{
		mbus:      mbus,
		store:     store,
		workflows: make(map[string]*workflowState),
		jobs:      make(map[uuid.UUID]*jobState),
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

func jsonMustMarshal(data map[string]json.RawMessage) []byte {
	result, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return result
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

func (e *engine) Update(config *config.Workflow) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
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
			Config:         *config,
			NodeSuccessors: makeSuccessors(config),
			TaskSuccessors: makeTaskSuccessors(config),
		}
		if len(config.VHosts) > 0 {
			wstate.VHost = config.VHosts[i]
		}
		e.workflows[name] = wstate
	}
	return nil
}

func (e *engine) Delete(name string) error { return nil }
func (e *engine) ListWorkflows() []string {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	names := make([]string, 0, len(e.workflows))
	for k := range e.workflows {
		names = append(names, k)
	}
	return names
}

func makeMessage(id uuid.UUID, nodeName string, data map[string]json.RawMessage) (string, map[string]json.RawMessage) {
	correlationId := id.String() + correlationIdSepToken + nodeName
	return correlationId, data
}

func getTaskConfig(wconfig *config.Workflow, taskName string) *config.Task {
	for _, taskConfig := range wconfig.Tasks {
		if taskConfig.Name == taskName {
			return taskConfig
		}
	}
	return nil
}

// If the raw json contains a dict with an "id" field and that is an uuid
// return the value.
func getJsonDictID(elementRaw json.RawMessage) (uuid.UUID, bool) {
	var element map[string]json.RawMessage
	if err := json.Unmarshal(elementRaw, &element); err != nil {
		return uuid.Nil, false
	}
	v, ok := element["id"]
	if !ok {
		return uuid.Nil, false
	}
	var idStr string
	if err := json.Unmarshal(v, &idStr); err != nil {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(idStr)
	return id, err == nil
}

func getTaskUUID(elementRaw json.RawMessage) uuid.UUID {
	if id, ok := getJsonDictID(elementRaw); ok {
		return id
	}
	return uuid.New()
}

func makeTaskMessage(inputData map[string]json.RawMessage, taskConfig *config.Task, elementRaw json.RawMessage) map[string]json.RawMessage {
	msg := make(map[string]json.RawMessage, len(inputData))
	for k, v := range inputData {
		if k == taskConfig.ItemListKey {
			continue
		}
		msg[k] = v
	}
	key := taskConfig.ItemListKey
	if len(taskConfig.ItemListKey) > 1 && strings.HasSuffix(taskConfig.ItemListKey, "s") {
		key = taskConfig.ItemListKey[:len(taskConfig.ItemListKey)-1]
	}
	msg[key] = elementRaw
	return msg
}

func (e *engine) createTask(job *jobState, nodeName, taskName string, data map[string]json.RawMessage) error {
	taskConfig := getTaskConfig(&job.Workflow.Config, taskName)
	elementsRaw, exists := data[taskConfig.ItemListKey]
	if !exists {
		return errTaskNop
	}
	var jsonElementList []json.RawMessage
	if err := json.Unmarshal(elementsRaw, &jsonElementList); err != nil {
		return errTaskNop
	}
	taskIDs := make([]uuid.UUID, 0, len(jsonElementList))
	for _, elementRaw := range jsonElementList {
		id := getTaskUUID(elementRaw)
		taskIDs = append(taskIDs, id)
	}

	job.Open = append(job.Open, &LogEntry{
		Step:     nodeName,
		Start:    time.Now(),
		Children: taskIDs,
	})

	for i, elementRaw := range jsonElementList {
		id := taskIDs[i]
		wstate := &workflowState{
			VHost:          job.Workflow.VHost,
			Name:           job.Workflow.Name + "/" + nodeName,
			Config:         job.Workflow.Config,
			NodeSuccessors: job.Workflow.TaskSuccessors[taskName],
			TaskSuccessors: job.Workflow.TaskSuccessors,
		}
		task := &jobState{
			ID:       id,
			Workflow: wstate,
			Closed:   make(map[string]*LogEntry),
			Task:     taskName,
			Parent:   job,
		}
		e.mutex.Lock()
		e.jobs[id] = task
		e.mutex.Unlock()

		msg := makeTaskMessage(data, taskConfig, elementRaw)
		e.workflowStart(task, msg)
	}
	return nil
}

func (e *engine) workflowStart(job *jobState, data map[string]json.RawMessage) error {
	wrk := job.Workflow
	succ, ok := wrk.NodeSuccessors[config.WorkflowStart]
	if !ok {
		return fmt.Errorf("invalid workflow: %s", wrk.Name)
	}

	job.mutex.Lock()
	defer job.mutex.Unlock()

	for nsuccessors := []*workflowNode{}; len(succ) > 0; succ, nsuccessors = nsuccessors, []*workflowNode{} {
		for _, node := range succ {
			if node.Task == "" {
				log := &LogEntry{
					Step:  node.Name,
					Start: time.Now(),
				}
				job.Open = append(job.Open, log)
				correlationId, msg := makeMessage(job.ID, node.Name, data)
				e.mbus.SendMsg(wrk.VHost, node.Queue, correlationId, msg)
			} else {
				err := e.createTask(job, node.Name, node.Task, data)
				if err == errTaskNop {
					// transition to successor of this node.
					job.Closed[node.Name] = &LogEntry{
						Step:  node.Name,
						Start: time.Now(),
						Data:  jsonMustMarshal(data),
					}
					nsuccessors = append(nsuccessors, wrk.NodeSuccessors[node.Name]...)
				}
			}
		}
	}
	return nil
}

func (e *engine) Create(workflow string, id uuid.UUID, dict map[string]json.RawMessage) error {
	if _, exists := e.jobs[id]; exists {
		return fmt.Errorf("duplicate item id: %v", id)
	}
	wstate, exists := e.workflows[workflow]
	if !exists {
		return fmt.Errorf("unknown workflow: %s", workflow)
	}

	e.mutex.Lock()
	e.jobs[id] = &jobState{
		ID:       id,
		Workflow: wstate,
		Closed:   make(map[string]*LogEntry),
	}
	e.mutex.Unlock()
	return e.workflowStart(e.jobs[id], dict)
}

func dependencyCheck(job *jobState, node *workflowNode) (bool, [][]byte) {
	jsonData := make([][]byte, 0, len(node.Ancestors))
	for _, dep := range node.Ancestors {
		if entry, ok := job.Closed[dep]; !ok {
			return false, nil
		} else {
			jsonData = append(jsonData, entry.Data)
		}
	}
	return true, jsonData
}

func mergeJsonDataDependents(jsonData [][]byte) map[string]json.RawMessage {
	merged := make(map[string]json.RawMessage)
	for _, encoded := range jsonData {
		var data map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &data); err != nil {
			continue
		}
		for k, v := range data {
			if _, exists := merged[k]; exists {
				continue
			}
			merged[k] = v
		}
	}
	return merged
}

// called with mutex locked
func (e *engine) advanceStateLocked(job *jobState, stepName string, changes []*LogEntry) error {
	wrk := job.Workflow
	succ, ok := wrk.NodeSuccessors[stepName]
	if !ok {
		return fmt.Errorf("internal error: %s no successors for node %s", wrk.Name, stepName)
	}

	for nsuccessors := []*workflowNode{}; len(succ) > 0; succ, nsuccessors = nsuccessors, []*workflowNode{} {
		for _, node := range succ {
			done, jsonData := dependencyCheck(job, node)
			if !done {
				continue
			}

			data := mergeJsonDataDependents(jsonData)
			if node.Name == config.WorkflowEnd {
				e.completed(job, data, changes)
				break
			}

			if node.Task == "" {
				log := &LogEntry{
					Step:  node.Name,
					Start: time.Now(),
				}
				job.Open = append(job.Open, log)
				correlationId, msg := makeMessage(job.ID, node.Name, data)
				e.mbus.SendMsg(wrk.VHost, node.Queue, correlationId, msg)
			} else {
				err := e.createTask(job, node.Name, node.Task, data)
				if err == errTaskNop {
					job.Closed[node.Name] = &LogEntry{
						Step:  node.Name,
						Start: time.Now(),
					}
					nsuccessors = append(nsuccessors, wrk.NodeSuccessors[node.Name]...)
				}
			}
		}
	}
	return nil
}

func (e *engine) advanceState(job *jobState, stepName string, changes []*LogEntry) error {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	return e.advanceStateLocked(job, stepName, changes)
}

func (e *engine) completed(job *jobState, data map[string]json.RawMessage, changes []*LogEntry) {
	log := LogEntry{
		Step:  config.WorkflowEnd,
		Start: time.Now(),
		Data:  jsonMustMarshal(data),
	}

	_ = append(changes, &log)
	job.Closed[config.WorkflowEnd] = &log

	if job.watcher != nil {
		job.watcher <- log
		close(job.watcher)
	}
	if job.Parent != nil {
		pieces := strings.Split(job.Workflow.Name, "/")
		nodeName := pieces[len(pieces)-1]
		e.maybeTaskEnd(job.Parent, nodeName, job.Task)
	}
}

func (e *engine) isTaskDone(taskIDs []uuid.UUID) (bool, [][]byte) {
	jsonData := make([][]byte, 0, len(taskIDs))
	e.mutex.Lock()
	defer e.mutex.Unlock()
	for _, id := range taskIDs {
		j := e.jobs[id]
		l, ok := j.Closed[config.WorkflowEnd]
		if !ok {
			return false, nil
		}
		jsonData = append(jsonData, l.Data)
	}
	return true, jsonData
}

func mergeTaskJsonData(jsonData [][]byte, taskConfig *config.Task) []byte {
	elementKey := taskConfig.ItemListKey
	if len(taskConfig.ItemListKey) > 1 && strings.HasSuffix(taskConfig.ItemListKey, "s") {
		elementKey = taskConfig.ItemListKey[:len(taskConfig.ItemListKey)-1]
	}

	msg := make(map[string]json.RawMessage)
	gather := make([]json.RawMessage, 0, len(jsonData))

	for _, rawData := range jsonData {
		var data map[string]json.RawMessage
		if err := json.Unmarshal(rawData, &data); err != nil {
			panic(err)
		}
		for k, v := range data {
			if k == elementKey {
				gather = append(gather, v)
				continue
			}
			if _, exists := msg[k]; exists {
				continue
			}
			msg[k] = v
		}
	}

	value, err := json.Marshal(gather)
	if err != nil {
		panic(err)
	}
	msg[taskConfig.ItemListKey] = value

	bytes, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (e *engine) maybeTaskEnd(job *jobState, nodeName, taskName string) {
	var done bool
	var jsonData [][]byte
	changes := make([]*LogEntry, 1, 2)

	job.mutex.Lock()
	defer job.mutex.Unlock()

	for i, logEntry := range job.Open {
		if logEntry.Step == nodeName {
			done, jsonData = e.isTaskDone(logEntry.Children)
			if done {
				job.Open = append(job.Open[:i], job.Open[i+1:]...)
				logEntry.End = time.Now()
				logEntry.Data = mergeTaskJsonData(
					jsonData, getTaskConfig(&job.Workflow.Config, taskName))
				job.Closed[logEntry.Step] = logEntry
				changes[0] = logEntry
			}
			break
		}
	}
	if !done {
		return
	}

	if err := e.advanceStateLocked(job, nodeName, changes); err != nil {
		log.Print(err)
	}
}

func updateLogEntryFromEvent(job *jobState, nodeName string, body []byte) *LogEntry {
	job.mutex.Lock()
	defer job.mutex.Unlock()

	var logEntry *LogEntry
	for i, entry := range job.Open {
		if entry.Step == nodeName {
			logEntry = entry
			job.Open = append(job.Open[:i], job.Open[i+1:]...)
			break
		}
	}

	if logEntry == nil {
		return nil
	}
	logEntry.End = time.Now()
	logEntry.Data = body
	job.Closed[logEntry.Step] = logEntry
	return logEntry
}

func (e *engine) OnEvent(correlationId string, body []byte) error {
	fields := strings.Split(correlationId, correlationIdSepToken)
	if len(fields) != 2 {
		return fmt.Errorf("invalid message: malformed correlationId %s", correlationId)
	}

	id, err := uuid.Parse(fields[0])
	if err != nil {
		return fmt.Errorf("invalid message id: %w", err)
	}

	var msg map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &msg); err != nil {
			return fmt.Errorf("event json decode: %w", err)
		}
	}

	e.mutex.Lock()
	job, exists := e.jobs[id]
	e.mutex.Unlock()
	if !exists {
		return fmt.Errorf("unknown job id: %v", id)
	}

	logEntry := updateLogEntryFromEvent(job, fields[1], body)
	if logEntry == nil {
		return fmt.Errorf("unexpected message for id %v from %s", id, fields[1])
	}

	if job.watcher != nil && job.watchAll {
		job.watcher <- *logEntry
	}

	changes := make([]*LogEntry, 1, 2)
	changes[0] = logEntry

	if err := e.advanceState(job, logEntry.Step, changes); err != nil {
		return err
	}

	if e.store != nil {
		if err := e.store.Update(job.ID, job.Workflow.Name, changes); err != nil {
			log.Print(err)
		}
	}

	return nil
}

func (e *engine) Watch(id uuid.UUID, allEvents bool, ch chan LogEntry) error {
	e.mutex.Lock()
	item, exists := e.jobs[id]
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

func (e *engine) Cancel(id uuid.UUID) error                             { return nil }
func (e *engine) ListWorkflowJobs(workflow string) ([]uuid.UUID, error) { return nil, nil }

func (e *engine) ListJobs() []uuid.UUID {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	jobIDs := make([]uuid.UUID, 0, len(e.jobs))
	for k := range e.jobs {
		jobIDs = append(jobIDs, k)
	}

	return jobIDs
}
