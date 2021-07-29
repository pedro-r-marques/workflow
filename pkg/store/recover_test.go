package store

import (
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pedro-r-marques/workflow/pkg/config"
	"github.com/pedro-r-marques/workflow/pkg/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mbusMessage struct {
	correlationId string
	msg           map[string]json.RawMessage
}
type mockBus struct {
	ch chan mbusMessage
}

func newMockBus() *mockBus {
	return &mockBus{make(chan mbusMessage, 8)}
}
func (b *mockBus) SetHandler(engine.MessageBusRecvHandler) {}
func (b *mockBus) VHostInit(vhost string) error            { return nil }

func (b *mockBus) SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error {
	b.ch <- mbusMessage{correlationId, msg}
	return nil
}

type readOnlyStore struct {
	store engine.JobStore
}

func (ro *readOnlyStore) Update(id uuid.UUID, workflow string, logs []*engine.LogEntry) error {
	panic("write operation not allowed")
	return nil
}
func (ro *readOnlyStore) OnJobDone(id uuid.UUID, workflow string, logs []*engine.LogEntry) error {
	panic("write operation not allowed")
	return nil
}

func (ro *readOnlyStore) GetRunningJobLogs(id uuid.UUID) (*engine.JobLogInfo, error) {
	return ro.store.GetRunningJobLogs(id)
}
func (ro *readOnlyStore) GetCompletedJobLogs(id uuid.UUID) (*engine.JobLogInfo, error) {
	return ro.store.GetCompletedJobLogs(id)
}
func (ro *readOnlyStore) Recover() ([]engine.JobLogInfo, error) {
	return ro.store.Recover()
}

func jsonMustMarshal(data map[string]json.RawMessage) []byte {
	result, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return result
}

func cloneSqliteStore(t *testing.T, src *sqliteStore) engine.JobStore {
	tmpfile, err := ioutil.TempFile("", "dbfile")
	if err != nil {
		t.Fatal(err)
	}
	// make sure writes are flushed.
	src.db.Close()

	fp, err := os.Open(src.Filename)
	defer fp.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.Copy(tmpfile, fp)
	if err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	src.db, err = sql.Open("sqlite3", src.DSN)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := NewSqliteStore(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func rmRunningJobFromStore(t *testing.T, store engine.JobStore, id uuid.UUID) {
	sqlStore := store.(*sqliteStore)
	r, err := sqlStore.db.Exec("DELETE FROM jobs_running WHERE uuid = ?", id)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := r.RowsAffected(); v == 0 {
		t.Fatal("now rows affected")
	}
}

func drainMessages(ch chan mbusMessage) []mbusMessage {
	var rcvMessages []mbusMessage
	for {
		select {
		case m := <-ch:
			rcvMessages = append(rcvMessages, m)
		default:
			return rcvMessages
		}
	}
}

func makeEngine(filename string, store engine.JobStore) (engine.WorkflowEngine, *mockBus, error) {
	mockBus := newMockBus()
	wrkEngine := engine.NewWorkflowEngine(mockBus, store)
	workflows, err := config.ParseConfig(filename)
	if err != nil {
		return nil, nil, err
	}
	if err := wrkEngine.Update(&workflows[0]); err != nil {
		return nil, nil, err
	}
	return wrkEngine, mockBus, nil
}

func recoverAndExpect(t *testing.T, store engine.JobStore, expectedMsgs []string, runToCompletion bool) {
	wrkEngine, mockBus, err := makeEngine("../engine/testdata/task.yaml", store)
	if err != nil {
		t.Fatal(err)
	}

	assert.NoError(t, wrkEngine.RecoverRunningJobs())
	var actualIDs []string
	var rcvMessages []mbusMessage
	for i := 0; i < len(expectedMsgs); i++ {
		m := <-mockBus.ch
		rcvMessages = append(rcvMessages, m)
		actualIDs = append(actualIDs, m.correlationId)
	}

	assert.ElementsMatch(t, expectedMsgs, actualIDs)
	select {
	case m := <-mockBus.ch:
		t.Error("unexpected message", m.correlationId)
	default:
		break
	}
	if !runToCompletion {
		return
	}

	for _, m := range rcvMessages {
		err := wrkEngine.OnEvent(m.correlationId, jsonMustMarshal(m.msg))
		assert.NoError(t, err)
	}
	assert.Greater(t, len(wrkEngine.ListJobs()), 0)
	running := true
	for running {
		select {
		case m := <-mockBus.ch:
			wrkEngine.OnEvent(m.correlationId, jsonMustMarshal(m.msg))
		default:
			running = false
		}
	}
	assert.Len(t, wrkEngine.ListJobs(), 0)
}

func TestJobRecovery(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "dbfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	store, err := NewSqliteStore(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	wrkEngine, mockBus, err := makeEngine("../engine/testdata/task.yaml", store)
	if err != nil {
		t.Fatal(err)
	}

	startMessage := map[string]json.RawMessage{
		"elements": json.RawMessage("[1, 2]"),
	}
	jobID := uuid.New()
	err = wrkEngine.Create("example", jobID, startMessage)
	assert.NoError(t, err)

	// recover a new engine from the saved data and expect it to send
	// a message to start step "s0"
	t.Run("step0", func(t *testing.T) {
		recoverAndExpect(t, &readOnlyStore{store}, []string{jobID.String() + ":s0"}, false)
	})

	// reply to s0
	m0 := <-mockBus.ch
	assert.NoError(t, wrkEngine.OnEvent(m0.correlationId, jsonMustMarshal(m0.msg)))

	// This causes 2 tasks to be created
	// The tasks may or not be in the logs
	t.Run("s1s2 start", func(t *testing.T) {
		// clone the store
		s2 := cloneSqliteStore(t, store.(*sqliteStore))
		// remove the 2nd task from the cloned log
		open, _, err := wrkEngine.JobStatus(jobID)
		if err != nil {
			t.Fatal(err)
		}
		require.Len(t, open, 2)
		var sOpen *engine.JobStatusEntry
		for _, e := range open {
			if e.Name == "s1" {
				sOpen = &e
				break
			}
		}
		require.NotNil(t, sOpen)
		require.Len(t, sOpen.Children, 2)
		rmRunningJobFromStore(t, s2, sOpen.Children[1])

		// Recover and expect 2 task starts
		expected := make([]string, 2)
		for i, tid := range open[0].Children {
			expected[i] = tid.String() + ":t1s1"
		}
		expected = append(expected, jobID.String()+":s2")
		recoverAndExpect(t, s2, expected, true)
		os.Remove(s2.(*sqliteStore).Filename)
	})

	// Ack one of the tasks
	rcvMessages := drainMessages(mockBus.ch)
	for ix, m := range rcvMessages {
		if strings.HasSuffix(m.correlationId, ":t1s1") {
			wrkEngine.OnEvent(m.correlationId, jsonMustMarshal(m.msg))
			rcvMessages = append(rcvMessages[:ix], rcvMessages[ix+1:]...)
			break
		}
	}

	// Recover expecting only the other task to restart
	t.Run("s1 task", func(t *testing.T) {
		s2 := cloneSqliteStore(t, store.(*sqliteStore))
		expected := make([]string, len(rcvMessages))
		for i, m := range rcvMessages {
			expected[i] = m.correlationId
		}
		recoverAndExpect(t, s2, expected, true)
		os.Remove(s2.(*sqliteStore).Filename)
	})

	// Ack the remaining task
	for ix, m := range rcvMessages {
		if strings.HasSuffix(m.correlationId, ":t1s1") {
			wrkEngine.OnEvent(m.correlationId, jsonMustMarshal(m.msg))
			rcvMessages = append(rcvMessages[:ix], rcvMessages[ix+1:]...)
			break
		}
	}

	// Recover expecting step s3 to start
	t.Run("s2s3", func(t *testing.T) {
		s2 := cloneSqliteStore(t, store.(*sqliteStore))
		expected := []string{
			jobID.String() + ":s2",
			jobID.String() + ":s3",
		}
		recoverAndExpect(t, s2, expected, false)
		os.Remove(s2.(*sqliteStore).Filename)
	})

	// Ack step2
	require.Len(t, rcvMessages, 1)
	{
		m := rcvMessages[0]
		wrkEngine.OnEvent(m.correlationId, jsonMustMarshal(m.msg))
	}

	t.Run("s3", func(t *testing.T) {
		s2 := cloneSqliteStore(t, store.(*sqliteStore))
		expected := []string{
			jobID.String() + ":s3",
		}
		recoverAndExpect(t, s2, expected, false)
		os.Remove(s2.(*sqliteStore).Filename)
	})

}
