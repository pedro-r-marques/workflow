package engine

//go:generate mockgen -source store.go -destination ./mock/store.go

import "github.com/google/uuid"

type JobLogInfo struct {
	ID       uuid.UUID
	Workflow string
	Logs     []*LogEntry
}

type JobStore interface {
	Update(id uuid.UUID, workflow string, logs []*LogEntry) error
	GetRunningJobLogs(id uuid.UUID) ([]*LogEntry, error)
	GetCompletedJobLogs(id uuid.UUID) ([]*LogEntry, error)
	OnJobDone(id uuid.UUID, workflow string, logs []*LogEntry) error
	Recover() ([]JobLogInfo, error)
}
