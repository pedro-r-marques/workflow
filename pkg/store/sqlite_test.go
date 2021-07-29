package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pedro-r-marques/workflow/pkg/engine"
)

func TestUpdate(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "dbfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	store, err := NewSqliteStore(tmpfile.Name())
	assert.NoError(t, err)

	jobID := uuid.New()
	logs := []*engine.LogEntry{
		{
			Step: "s0",
		},
	}
	err = store.Update(jobID, "example-workflow", logs)
	require.NoError(t, err)
}

func TestUpdateAndRead(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "dbfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	store, err := NewSqliteStore(tmpfile.Name())
	assert.NoError(t, err)

	jobID := uuid.New()
	logs := []*engine.LogEntry{
		{
			Step:   "s0",
			Worker: "w1",
		},
	}
	err = store.Update(jobID, "example-workflow", logs)
	require.NoError(t, err)

	logs2 := []*engine.LogEntry{
		{
			Step:   "s0",
			Worker: "w2",
		},
		{
			Step:   "s1",
			Worker: "w3",
		},
	}
	err = store.Update(jobID, "example-workflow", logs2)
	require.NoError(t, err)

	jobInfo, err := store.GetRunningJobLogs(jobID)
	assert.NoError(t, err)
	require.Equal(t, "example-workflow", jobInfo.Workflow)
	require.Len(t, jobInfo.Logs, 2)
	require.ElementsMatch(t, logs2, jobInfo.Logs)
}

func TestJobDone(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "dbfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	store, err := NewSqliteStore(tmpfile.Name())
	assert.NoError(t, err)

	var jobIDs []uuid.UUID

	for i := 0; i < 10; i++ {
		jobID := uuid.New()
		logs := []*engine.LogEntry{
			{
				Step:   "s0",
				Worker: fmt.Sprintf("w%d", i),
			},
		}
		err = store.Update(jobID, "example-workflow", logs)
		require.NoError(t, err)
		jobIDs = append(jobIDs, jobID)
	}

	indices := []int{3, 5, 7}
	for _, ix := range indices {
		jobInfo, err := store.GetRunningJobLogs(jobIDs[ix])
		require.NoError(t, err)
		err = store.OnJobDone(jobIDs[ix], "example-workflow", jobInfo.Logs)
		require.NoError(t, err)
	}

	for _, ix := range indices {
		_, err := store.GetRunningJobLogs(jobIDs[ix])
		require.Error(t, err)
	}

	_, err = store.GetRunningJobLogs(jobIDs[0])
	require.NoError(t, err)

	for _, ix := range indices {
		jobInfo, err := store.GetCompletedJobLogs(jobIDs[ix])
		require.NoError(t, err)
		require.Len(t, jobInfo.Logs, 1)
	}
}

func TestRecover(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "dbfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	store, err := NewSqliteStore(tmpfile.Name())
	assert.NoError(t, err)

	var jobIDs []uuid.UUID

	steps := []string{"s0", "s1", "s3"}
	for n, step := range steps {
		for i := 0; i < 10; i++ {
			var jobID uuid.UUID
			if n == 0 {
				jobID = uuid.New()
				jobIDs = append(jobIDs, jobID)
			} else {
				jobID = jobIDs[i]
			}

			logs := []*engine.LogEntry{
				{
					Step:   step,
					Worker: fmt.Sprintf("w%d", i),
				},
			}
			err = store.Update(jobID, "example-workflow", logs)
			require.NoError(t, err)
		}
	}

	rcvInfo, err := store.Recover()
	assert.NoError(t, err)
	require.Len(t, rcvInfo, len(jobIDs))
	rcvIDs := make([]uuid.UUID, len(jobIDs))
	for i, l := range rcvInfo {
		rcvIDs[i] = l.ID
		require.Len(t, l.Logs, len(steps))
	}
	require.ElementsMatch(t, rcvIDs, jobIDs)
}
