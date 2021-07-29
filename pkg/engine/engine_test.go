package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pedro-r-marques/workflow/pkg/config"
)

type nopStore struct {
}

func (s *nopStore) Update(id uuid.UUID, workflow string, logs []*LogEntry) error    { return nil }
func (s *nopStore) GetRunningJobLogs(id uuid.UUID) (*JobLogInfo, error)             { return nil, nil }
func (s *nopStore) GetCompletedJobLogs(id uuid.UUID) (*JobLogInfo, error)           { return nil, nil }
func (s *nopStore) OnJobDone(id uuid.UUID, workflow string, logs []*LogEntry) error { return nil }
func (s *nopStore) Recover() ([]JobLogInfo, error) {
	return nil, nil
}

type nopBus struct {
	messages []map[string]json.RawMessage
}

func (b *nopBus) SetHandler(MessageBusRecvHandler) {}
func (b *nopBus) VHostInit(vhost string) error     { return nil }
func (b *nopBus) SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error {
	b.messages = append(b.messages, msg)
	return nil
}

func TestSimpleStateMachine(t *testing.T) {
	workflows, err := config.ParseConfig("testdata/simple.yaml")
	require.NoError(t, err)
	require.Len(t, workflows, 1)

	engine := NewWorkflowEngine(&nopBus{}, &nopStore{})
	if err := engine.Update(&workflows[0]); err != nil {
		t.Fatal(err)
	}

	sequences := [][]string{
		{"s1", "s2", "s3", "s4"},
		{"s1", "s3", "s4", "s2"},
	}

	for _, sequence := range sequences {
		itemID, err := uuid.NewRandom()
		require.NoError(t, err)

		err = engine.Create(workflows[0].Name, itemID, map[string]json.RawMessage{})
		assert.NoError(t, err)

		ch := make(chan LogEntry, 1)
		err = engine.Watch(itemID, false, ch)
		assert.NoError(t, err)

		for _, event := range sequence {
			correlationId := itemID.String() + correlationIdSepToken + event
			msg := map[string]json.RawMessage{
				"worker": json.RawMessage("testing"),
			}
			body, _ := json.Marshal(msg)
			err := engine.OnEvent(correlationId, body)
			assert.NoError(t, err)
		}

		log := <-ch
		assert.Equal(t, config.WorkflowEnd, log.Step)
	}
}

func TestSuccessorNodes(t *testing.T) {
	eng := NewWorkflowEngine(&nopBus{}, &nopStore{})

	workflows, err := config.ParseConfig("testdata/mpath.yaml")
	require.NoError(t, err)
	require.Len(t, workflows, 1)

	if err := eng.Update(&workflows[0]); err != nil {
		t.Fatal(err)
	}

	impl := eng.(*engine)
	successors := impl.workflows["example"].NodeSuccessors
	assert.Contains(t, successors, config.WorkflowStart)

	for k, v := range successors {
		var nodeNames []string
		for _, node := range v {
			nodeNames = append(nodeNames, node.Name)
		}
		fmt.Printf("%s [%s]\n", k, strings.Join(nodeNames, ","))
	}
	assert.Len(t, successors, 5)
	assert.Len(t, impl.workflows["example"].TaskSuccessors, 2)
}

type mbusMessage struct {
	correlationId string
	msg           map[string]json.RawMessage
}
type mockBus struct {
	ch chan mbusMessage
}

func (b *mockBus) SetHandler(MessageBusRecvHandler) {}
func (b *mockBus) VHostInit(vhost string) error     { return nil }

func (b *mockBus) SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error {
	b.ch <- mbusMessage{correlationId, msg}
	return nil
}

func jobIDfromCorrelationID(correlationId string) uuid.UUID {
	loc := strings.Index(correlationId, ":")
	id, _ := uuid.Parse(correlationId[:loc])
	return id
}
func TestStartTask(t *testing.T) {
	mock := mockBus{
		ch: make(chan mbusMessage, 3),
	}
	eng := NewWorkflowEngine(&mock, &nopStore{})

	workflows, err := config.ParseConfig("testdata/task.yaml")
	require.NoError(t, err)
	require.Len(t, workflows, 1)

	if err := eng.Update(&workflows[0]); err != nil {
		t.Fatal(err)
	}

	jobID := uuid.New()
	err = eng.Create(workflows[0].Name, jobID, map[string]json.RawMessage{})
	assert.NoError(t, err)

	m0 := <-mock.ch
	require.Equal(t, jobID, jobIDfromCorrelationID(m0.correlationId))

	r0 := struct {
		Elements []int `json:"elements"`
	}{
		Elements: []int{1, 2},
	}
	encoded, _ := json.Marshal(r0)
	err = eng.OnEvent(m0.correlationId, encoded)
	assert.NoError(t, err)

	var s2Start bool
	var task1Start bool
	for i := 0; i < 3; i++ {
		msg := <-mock.ch
		fmt.Println(msg)
		if msg.correlationId == jobID.String()+correlationIdSepToken+"s2" {
			s2Start = true
		}
		if strings.HasSuffix(msg.correlationId, correlationIdSepToken+"t1s1") {
			task1Start = true
		}
	}

	require.True(t, s2Start)
	require.True(t, task1Start)
}

func TestEndTask(t *testing.T) {
	mock := mockBus{
		ch: make(chan mbusMessage, 3),
	}
	eng := NewWorkflowEngine(&mock, &nopStore{})

	workflows, err := config.ParseConfig("testdata/task.yaml")
	require.NoError(t, err)
	require.Len(t, workflows, 1)

	if err := eng.Update(&workflows[0]); err != nil {
		t.Fatal(err)
	}

	jobID := uuid.New()
	err = eng.Create(workflows[0].Name, jobID, map[string]json.RawMessage{})
	assert.NoError(t, err)

	m0 := <-mock.ch
	require.Equal(t, jobID, jobIDfromCorrelationID(m0.correlationId))

	r0 := struct {
		Elements []int `json:"elements"`
	}{
		Elements: []int{1, 2},
	}
	encoded, _ := json.Marshal(r0)
	err = eng.OnEvent(m0.correlationId, encoded)
	assert.NoError(t, err)

	// Reply to the task1 messages
	for i := 0; i < 3; i++ {
		msg := <-mock.ch
		if !strings.HasSuffix(msg.correlationId, correlationIdSepToken+"t1s1") {
			continue
		}
		reply := struct {
			Element int `json:"element"`
		}{
			Element: i,
		}
		encoded, _ := json.Marshal(reply)
		err = eng.OnEvent(msg.correlationId, encoded)
		assert.NoError(t, err)
	}

	m2 := <-mock.ch
	require.Equal(t, jobID.String()+correlationIdSepToken+"s3", m2.correlationId)
}

// Ensure that the state machine advances if there are no task elements.
func TestTaskSkip(t *testing.T) {

}

func TestDuplicateResponse(t *testing.T) {

}

func TestJobList(t *testing.T) {
	mock := mockBus{
		ch: make(chan mbusMessage, 3),
	}
	eng := NewWorkflowEngine(&mock, &nopStore{})

	workflows, err := config.ParseConfig("testdata/task.yaml")
	require.NoError(t, err)
	require.Len(t, workflows, 1)

	if err := eng.Update(&workflows[0]); err != nil {
		t.Fatal(err)
	}

	jobID := uuid.New()
	err = eng.Create(workflows[0].Name, jobID, map[string]json.RawMessage{})
	assert.NoError(t, err)

	require.Len(t, eng.ListJobs(), 1)
	uuids, err := eng.ListWorkflowJobs(workflows[0].Name)
	require.NoError(t, err)
	require.Len(t, uuids, 1)

	m0 := <-mock.ch
	require.Equal(t, jobID, jobIDfromCorrelationID(m0.correlationId))

	// Reply from step "s0" such that 2 tasks created.
	r0 := struct {
		Elements []int `json:"elements"`
	}{
		Elements: []int{1, 2},
	}
	encoded, _ := json.Marshal(r0)
	err = eng.OnEvent(m0.correlationId, encoded)
	assert.NoError(t, err)

	require.Len(t, eng.ListJobs(), 3)

	// Reply to all messages
	for i := 0; i < 4; i++ {
		msg := <-mock.ch
		encoded, _ := json.Marshal(msg.msg)
		require.NoError(t, eng.OnEvent(msg.correlationId, encoded))
	}

	require.Len(t, eng.ListJobs(), 0)
	uuids, err = eng.ListWorkflowJobs(workflows[0].Name)
	require.NoError(t, err)
	require.Len(t, uuids, 0)
}

// echoBus reflects back the incoming message using a separate thread
type echoBus struct {
	engine             WorkflowEngine
	recvCorrelationIds []string
	ch                 chan mbusMessage
}

func newEchoBus() *echoBus {
	return &echoBus{
		ch: make(chan mbusMessage, 10),
	}
}
func (b *echoBus) SetHandler(MessageBusRecvHandler) {}
func (b *echoBus) VHostInit(vhost string) error     { return nil }

func (b *echoBus) SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error {
	b.recvCorrelationIds = append(b.recvCorrelationIds, correlationId)
	b.ch <- mbusMessage{correlationId, msg}
	return nil
}

func (b *echoBus) Run() {
	for {
		m, ok := <-b.ch
		if !ok {
			break
		}
		if err := b.engine.OnEvent(m.correlationId, jsonMustMarshal(m.msg)); err != nil {
			log.Print(err)
		}
	}
}

func TestSubtaskStateMachine(t *testing.T) {
	workflows, err := config.ParseConfig("testdata/mpath.yaml")
	require.NoError(t, err)
	require.Len(t, workflows, 1)

	mbus := newEchoBus()
	engine := NewWorkflowEngine(mbus, &nopStore{})
	mbus.engine = engine

	if err := engine.Update(&workflows[0]); err != nil {
		t.Fatal(err)
	}

	jobID, err := uuid.NewRandom()
	require.NoError(t, err)

	data := map[string]json.RawMessage{
		"k1": json.RawMessage("[1, 2]"),
	}
	err = engine.Create(workflows[0].Name, jobID, data)
	assert.NoError(t, err)

	ch := make(chan LogEntry, 1)
	err = engine.Watch(jobID, false, ch)
	assert.NoError(t, err)

	go mbus.Run()

	log := <-ch
	assert.Equal(t, config.WorkflowEnd, log.Step)
	assert.Len(t, mbus.recvCorrelationIds, 7)
}
