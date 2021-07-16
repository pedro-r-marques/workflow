package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pedro-r-marques/workflow/pkg/config"
)

type nopStore struct {
}

func (s *nopStore) Update(id uuid.UUID, workflow string, logs []*LogEntry) error {
	return nil
}

type nopBus struct {
	messages []map[string]json.RawMessage
}

func (b *nopBus) SendMsg(vhost string, qname string, msg map[string]json.RawMessage) error {
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
			msg := map[string]json.RawMessage{
				"id":     json.RawMessage(itemID.String()),
				"node":   json.RawMessage(event),
				"worker": json.RawMessage("testing"),
			}
			err := engine.OnEvent(msg)
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
		var ancestors string
		for n, node := range v {
			nodeNames = append(nodeNames, node.Name)
			if n > 0 {
				ancestors += " "
			}
			ancestors += "{" + strings.Join(node.Ancestors, ", ") + "}"
		}
		fmt.Printf("%s [%s] %s\n", k, strings.Join(nodeNames, ","), ancestors)
	}
	assert.Len(t, successors, 4)
}

// echoBus reflects back the incoming message using a separate thread
type echoBus struct {
	engine   WorkflowEngine
	messages []map[string]json.RawMessage
	ch       chan map[string]json.RawMessage
}

func newEchoBus() *echoBus {
	return &echoBus{
		ch: make(chan map[string]json.RawMessage, 2),
	}
}
func (b *echoBus) SendMsg(vhost string, qname string, msg map[string]json.RawMessage) error {
	b.messages = append(b.messages, msg)
	b.ch <- msg
	return nil
}

func (b *echoBus) Run() {
	for {
		msg, ok := <-b.ch
		if !ok {
			break
		}
		b.engine.OnEvent(msg)
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

	itemID, err := uuid.NewRandom()
	require.NoError(t, err)

	err = engine.Create(workflows[0].Name, itemID, map[string]json.RawMessage{})
	assert.NoError(t, err)

	ch := make(chan LogEntry, 1)
	err = engine.Watch(itemID, false, ch)
	assert.NoError(t, err)

	go mbus.Run()

	log := <-ch
	assert.Equal(t, config.WorkflowEnd, log.Step)
	assert.Len(t, mbus.messages, 3)
}
