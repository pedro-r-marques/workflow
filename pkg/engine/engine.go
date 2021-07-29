package engine

//go:generate mockgen -source engine.go -destination ./mock/engine.go

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/pedro-r-marques/workflow/pkg/config"
)

const correlationIdSepToken = ":"

var errTaskNop = errors.New("no elements for task")

// Information returned by ListWorkflows per workflow
type WorkflowInfo struct {
	VHost    string `json:"vhost"`
	Name     string `json:"name"`
	JobCount int    `json:"job_count"`
}

type WorkflowEngine interface {
	// Workflow configuration
	Update(*config.Workflow) error
	Delete(name string) error
	ListWorkflows() []WorkflowInfo

	// Workflow job
	Create(workflow string, id uuid.UUID, dict map[string]json.RawMessage) error
	Cancel(id uuid.UUID) error
	OnEvent(correlationId string, body []byte) error
	Watch(id uuid.UUID, allEvents bool, ch chan LogEntry) error
	ListWorkflowJobs(workflow string) ([]uuid.UUID, error)
	ListJobs() []uuid.UUID
	JobStatus(id uuid.UUID) ([]JobStatusEntry, []JobStatusEntry, error)

	RecoverRunningJobs() error
}

type engine struct {
	mbus      MessageBus
	store     JobStore
	workflows map[string]*workflowState
	jobs      map[uuid.UUID]*jobState
	mutex     sync.Mutex
}

type changeList struct {
	logs []*LogEntry
}

func (c *changeList) add(LogEntry *LogEntry) {
	c.logs = append(c.logs, LogEntry)
}

func (c *changeList) clear() {
	c.logs = nil
}
func (c *changeList) isEmpty() bool {
	return len(c.logs) == 0
}

func NewWorkflowEngine(mbus MessageBus, store JobStore) WorkflowEngine {
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

func (e *engine) Update(wconfig *config.Workflow) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	// TODO: garbage collect previous vhosts.
	var workflowNames []string
	if len(wconfig.VHosts) > 0 {
		workflowNames = make([]string, len(wconfig.VHosts))
		for i, vhost := range wconfig.VHosts {
			workflowNames[i] = strings.Join([]string{vhost, wconfig.Name}, "/")
		}
	} else {
		workflowNames = []string{wconfig.Name}
	}
	for i, name := range workflowNames {
		wstate := makeWorkflowState(wconfig)
		if len(wconfig.VHosts) > 0 {
			wstate.VHost = wconfig.VHosts[i]
		}
		e.workflows[name] = wstate
	}
	return nil
}

func (e *engine) Delete(name string) error { return nil }

func (e *engine) ListWorkflows() []WorkflowInfo {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	result := make([]WorkflowInfo, 0, len(e.workflows))
	for _, v := range e.workflows {
		v.mutex.Lock()
		defer v.mutex.Unlock()
		result = append(result, WorkflowInfo{
			VHost:    v.VHost,
			Name:     v.Name,
			JobCount: len(v.jobs),
		})
	}
	return result
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

func (e *engine) createTask(job *jobState, nodeName, taskName string, data map[string]json.RawMessage, changes *changeList) error {
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

	logEntry := &LogEntry{
		Step:     nodeName,
		Start:    time.Now(),
		Children: taskIDs,
		Data:     jsonMustMarshal(data),
	}
	job.Open = append(job.Open, logEntry)
	changes.add(logEntry)

	log.Debug().
		Str("id", job.ID.String()).
		Str("task", taskName).
		Msg("create task")

	for i, elementRaw := range jsonElementList {
		id := taskIDs[i]
		wstate := job.Workflow.locateTaskState(nodeName, taskName)
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

		wstate.onJobCreate(task)

		msg := makeTaskMessage(data, taskConfig, elementRaw)
		e.workflowStart(task, msg)
	}
	return nil
}

func (e *engine) workflowStart(job *jobState, data map[string]json.RawMessage) error {
	wrk := job.Workflow
	if _, ok := wrk.NodeSuccessors[config.WorkflowStart]; !ok {
		return fmt.Errorf("invalid workflow: %s", wrk.Name)
	}

	logEntry := &LogEntry{
		Step:  config.WorkflowStart,
		Start: time.Now(),
		End:   time.Now(),
		Data:  jsonMustMarshal(data),
	}

	var changes changeList
	changes.add(logEntry)
	job.mutex.Lock()
	defer job.mutex.Unlock()

	job.Closed[config.WorkflowStart] = logEntry

	e.advanceStateLocked(job, config.WorkflowStart, &changes)

	if e.store != nil {
		if err := e.store.Update(job.ID, job.Workflow.Name, changes.logs); err != nil {
			log.Error().Err(err)
		}
	}

	return nil
}

func (e *engine) Create(workflow string, id uuid.UUID, dict map[string]json.RawMessage) error {
	e.mutex.Lock()
	_, exists := e.jobs[id]
	e.mutex.Unlock()
	if exists {
		return fmt.Errorf("duplicate item id: %v", id)
	}

	e.mutex.Lock()
	wstate, exists := e.workflows[workflow]
	e.mutex.Unlock()
	if !exists {
		return fmt.Errorf("unknown workflow: %s", workflow)
	}

	log.Debug().
		Str("id", id.String()).
		Msg("create job")

	job := &jobState{
		ID:       id,
		Workflow: wstate,
		Closed:   make(map[string]*LogEntry),
	}

	e.mutex.Lock()
	e.jobs[id] = job
	e.mutex.Unlock()

	wstate.onJobCreate(job)
	return e.workflowStart(job, dict)
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

func inOpenList(job *jobState, nodeName string) bool {
	for _, entry := range job.Open {
		if entry.Step == nodeName {
			return true
		}
	}
	return false
}

// called with mutex locked
func (e *engine) advanceStateLocked(job *jobState, stepName string, changes *changeList) error {
	wrk := job.Workflow
	succ, ok := wrk.NodeSuccessors[stepName]
	if !ok {
		return fmt.Errorf("internal error: %s no successors for node %s", wrk.Name, stepName)
	}

	for nsuccessors := []*workflowNode{}; len(succ) > 0; succ, nsuccessors = nsuccessors, []*workflowNode{} {
		for _, node := range succ {
			if _, present := job.Closed[node.Name]; present {
				nsuccessors = append(nsuccessors, wrk.NodeSuccessors[node.Name]...)
				continue
			}
			if inOpenList(job, node.Name) {
				continue
			}
			done, jsonData := dependencyCheck(job, node)
			if !done {
				continue
			}

			data := mergeJsonDataDependents(jsonData)
			if node.Name == config.WorkflowEnd {
				e.completed(job, data, changes)
				break
			}

			log.Debug().
				Str("id", job.ID.String()).
				Str("state", node.Name).
				Msg("advance state")

			if node.Task == "" {
				log := &LogEntry{
					Step:  node.Name,
					Start: time.Now(),
				}
				job.Open = append(job.Open, log)
				correlationId, msg := makeMessage(job.ID, node.Name, data)
				e.mbus.SendMsg(wrk.VHost, node.Queue, correlationId, msg)
			} else {
				err := e.createTask(job, node.Name, node.Task, data, changes)
				if err == errTaskNop {
					logEntry := &LogEntry{
						Step:  node.Name,
						Start: time.Now(),
					}
					job.Closed[node.Name] = logEntry
					changes.add(logEntry)
					nsuccessors = append(nsuccessors, wrk.NodeSuccessors[node.Name]...)
				}
			}
		}
	}
	return nil
}

func (e *engine) advanceState(job *jobState, stepName string, changes *changeList) error {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	return e.advanceStateLocked(job, stepName, changes)
}

func (e *engine) completed(job *jobState, data map[string]json.RawMessage, changes *changeList) {
	logEntry := &LogEntry{
		Step:  config.WorkflowEnd,
		Start: time.Now(),
		End:   time.Now(),
		Data:  jsonMustMarshal(data),
	}

	job.Closed[config.WorkflowEnd] = logEntry

	job.Workflow.onJobDone(job)

	if job.watcher != nil {
		job.watcher <- *logEntry
		close(job.watcher)
	}

	if e.store != nil {
		logs := make([]*LogEntry, 0, len(job.Closed))
		for _, v := range job.Closed {
			logs = append(logs, v)
		}
		e.store.OnJobDone(job.ID, job.Workflow.Name, logs)
	}

	changes.clear()

	log.Debug().
		Str("id", job.ID.String()).
		Msg("job done")

	if job.Parent != nil {
		pieces := strings.Split(job.Workflow.Name, "/")
		nodeName := pieces[len(pieces)-1]
		e.maybeTaskEnd(job.Parent, nodeName, job.Task)
	} else {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		for _, l := range job.Closed {
			for _, id := range l.Children {
				delete(e.jobs, id)
			}
		}
		delete(e.jobs, job.ID)
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
	var changes changeList

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
				changes.add(logEntry)
			}
			break
		}
	}
	if !done {
		return
	}

	log.Debug().
		Str("id", job.ID.String()).
		Str("task", taskName).
		Msg("task done")

	if err := e.advanceStateLocked(job, nodeName, &changes); err != nil {
		log.Error().Err(err)
	}
	if e.store != nil && !changes.isEmpty() {
		e.store.Update(job.ID, job.Workflow.Name, changes.logs)
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

	var changes changeList
	changes.add(logEntry)

	if err := e.advanceState(job, logEntry.Step, &changes); err != nil {
		return err
	}

	if e.store != nil && !changes.isEmpty() {
		if err := e.store.Update(job.ID, job.Workflow.Name, changes.logs); err != nil {
			log.Error().Err(err)
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

func (e *engine) Cancel(id uuid.UUID) error { return nil }

func (e *engine) ListWorkflowJobs(workflowName string) ([]uuid.UUID, error) {
	e.mutex.Lock()
	wstate, exists := e.workflows[workflowName]
	e.mutex.Unlock()

	if !exists {
		return nil, fmt.Errorf("unknown workflow %s", workflowName)
	}
	wstate.mutex.Lock()
	defer wstate.mutex.Unlock()

	uuids := make([]uuid.UUID, len(wstate.jobs))
	for i, id := range wstate.jobs {
		uuids[i] = id
	}
	return uuids, nil
}

func (e *engine) ListJobs() []uuid.UUID {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	jobIDs := make([]uuid.UUID, 0, len(e.jobs))
	for k := range e.jobs {
		jobIDs = append(jobIDs, k)
	}

	return jobIDs
}

func elapsedTime(end, start time.Time) time.Duration {
	if end.IsZero() {
		return 0
	}
	return end.Sub(start)
}

func makeStatusEntry(logEntry *LogEntry) JobStatusEntry {
	return JobStatusEntry{
		Name:     logEntry.Step,
		Start:    logEntry.Start,
		Elapsed:  elapsedTime(logEntry.End, logEntry.Start),
		Children: logEntry.Children,
		Worker:   logEntry.Worker,
		Data:     logEntry.Data,
	}
}

func (e *engine) JobStatus(id uuid.UUID) ([]JobStatusEntry, []JobStatusEntry, error) {
	e.mutex.Lock()
	job, exists := e.jobs[id]
	e.mutex.Unlock()
	if !exists {
		return nil, nil, fmt.Errorf("unknown job id %v", id)
	}

	job.mutex.Lock()
	defer job.mutex.Unlock()

	open := make([]JobStatusEntry, 0, len(job.Open))
	for _, logEntry := range job.Open {
		open = append(open, makeStatusEntry(logEntry))
	}
	closed := make([]JobStatusEntry, 0, len(job.Closed))
	for _, logEntry := range job.Closed {
		closed = append(closed, makeStatusEntry(logEntry))
	}

	return open, closed, nil
}

func getJobStepTaskName(job *jobState, logEntry *LogEntry) (string, error) {
	for _, step := range job.Workflow.Config.Steps {
		if step.Name == logEntry.Step {
			return step.Task, nil
		}
	}
	return "", fmt.Errorf("step %s not found", logEntry.Step)
}

func getJobTaskCreateData(job *jobState, taskName string, logEntry *LogEntry) ([]json.RawMessage, error) {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(logEntry.Data, &data); err != nil {
		return nil, err
	}
	taskConfig := getTaskConfig(&job.Workflow.Config, taskName)
	elementsRaw, exists := data[taskConfig.ItemListKey]
	if !exists {
		return nil, errTaskNop
	}
	var jsonElementList []json.RawMessage
	if err := json.Unmarshal(elementsRaw, &jsonElementList); err != nil {
		return nil, errTaskNop
	}
	return jsonElementList, nil
}

func (e *engine) recreateTaskJob(job *jobState, stepName, taskName string, jsonElementList []json.RawMessage, ix int, tid uuid.UUID) error {
	wstate := job.Workflow.locateTaskState(stepName, taskName)

	data, err := json.Marshal(jsonElementList[ix])
	if err != nil {
		return err
	}

	logEntry := &LogEntry{
		Step:  config.WorkflowStart,
		Start: time.Now(),
		End:   time.Now(),
		Data:  data,
	}

	task := &jobState{
		ID:       tid,
		Workflow: wstate,
		Closed: map[string]*LogEntry{
			config.WorkflowStart: logEntry,
		},
		Task:   taskName,
		Parent: job,
	}
	e.jobs[tid] = task

	log.Info().
		Str("id", job.ID.String()).
		Str("taskJob", tid.String()).
		Msg("task job recreate")

	e.store.Update(tid, task.Workflow.Name, []*LogEntry{logEntry})

	wstate.onJobCreate(task)
	return nil
}

// recover a task based on storage logs
// the task may be either running or terminated
func (e *engine) recoverTaskJob(job *jobState, stepName, taskName string, taskInfo *JobLogInfo) error {
	wstate := job.Workflow.locateTaskState(stepName, taskName)
	var open []*LogEntry
	closed := make(map[string]*LogEntry, len(taskInfo.Logs))
	for _, l := range taskInfo.Logs {
		if l.End.IsZero() {
			open = append(open, l)
		} else {
			closed[l.Step] = l
		}
	}
	task := &jobState{
		ID:       taskInfo.ID,
		Workflow: wstate,
		Open:     open,
		Closed:   closed,
		Task:     taskName,
		Parent:   job,
	}
	e.jobs[taskInfo.ID] = task

	wstate.onJobCreate(task)
	return nil
}

func (e *engine) RecoverRunningJobs() error {
	jobInfo, err := e.store.Recover()
	if err != nil {
		return err
	}
	jobInfoByID := make(map[uuid.UUID]*JobLogInfo, len(jobInfo))

	topLevel := make([]*JobLogInfo, 0, len(jobInfo))
	taskIDs := make([]uuid.UUID, 0, len(jobInfo))

	for i := 0; i < len(jobInfo); i++ {
		ji := &jobInfo[i]
		jobInfoByID[ji.ID] = ji

		if _, exists := e.workflows[ji.Workflow]; exists {
			topLevel = append(topLevel, ji)
		}
	}

	for _, ji := range topLevel {
		wstate := e.workflows[ji.Workflow]
		job := &jobState{
			ID:       ji.ID,
			Workflow: wstate,
			Closed:   make(map[string]*LogEntry),
		}

		log.Info().
			Str("id", job.ID.String()).
			Msg("job recover")

		for _, l := range ji.Logs {
			if l.End.IsZero() {
				job.Open = append(job.Open, l)
			} else {
				job.Closed[l.Step] = l
				continue
			}

			taskName, err := getJobStepTaskName(job, l)
			if err != nil {
				log.Error().Err(err)
				continue
			}
			if taskName == "" {
				continue
			}
			var jsonElementList []json.RawMessage

			// read in completed tasks that we may depend on
			for ix, tid := range l.Children {
				if jobInfo, inMap := jobInfoByID[tid]; inMap {
					log.Debug().
						Str("id", job.ID.String()).
						Str("taskJob", tid.String()).
						Msg("recover running task")

					e.recoverTaskJob(job, l.Step, taskName, jobInfo)
					continue
				}
				// restore from storage log.
				if jobInfo, err := e.store.GetCompletedJobLogs(tid); err == nil {
					log.Debug().
						Str("id", job.ID.String()).
						Str("taskJob", tid.String()).
						Msg("recover completed task")

					e.recoverTaskJob(job, l.Step, taskName, jobInfo)
					continue
				}

				// missing task must be recreated
				if jsonElementList == nil {
					jsonElementList, err = getJobTaskCreateData(job, taskName, l)
					if err != nil {
						log.Error().Err(err)
						continue
					}
				}
				e.recreateTaskJob(job, l.Step, taskName, jsonElementList, ix, tid)
			}
			taskIDs = append(taskIDs, l.Children...)
		}

		e.jobs[ji.ID] = job
	}

	// advance state machine for top level jobs
	for _, ji := range topLevel {
		job := e.jobs[ji.ID]
		var nopChanges changeList
		e.advanceStateLocked(job, config.WorkflowStart, &nopChanges)
	}

	// advance state machine for tasks
	for _, tid := range taskIDs {
		task, exists := e.jobs[tid]
		if !exists {
			log.Error().Msgf("unable to recover task %v", tid)
			continue
		}
		var nopChanges changeList
		e.advanceStateLocked(task, config.WorkflowStart, &nopChanges)
	}

	return nil
}
