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

	rdLogs, err := store.GetRunningJobLogs(jobID)
	assert.NoError(t, err)
	require.Len(t, rdLogs, 2)
	require.ElementsMatch(t, logs2, rdLogs)
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
		logs, err := store.GetRunningJobLogs(jobIDs[ix])
		require.NoError(t, err)
		err = store.OnJobDone(jobIDs[ix], "example-workflow", logs)
		require.NoError(t, err)
	}

	for _, ix := range indices {
		_, err := store.GetRunningJobLogs(jobIDs[ix])
		require.Error(t, err)
	}

	_, err = store.GetRunningJobLogs(jobIDs[0])
	require.NoError(t, err)

	for _, ix := range indices {
		logs, err := store.GetCompletedJobLogs(jobIDs[ix])
		require.NoError(t, err)
		require.Len(t, logs, 1)
	}
}
