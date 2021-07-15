package engine

import (
	"encoding/json"
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
