package store

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	"github.com/pedro-r-marques/workflow/pkg/engine"
)

var ErrNoEntries = errors.New("no log entries for job")

type sqliteStore struct {
	Filename string
	DSN      string
	db       *sql.DB // sqlite3 supports a single write connection
	mutex    sync.Mutex
	sysClock func() time.Time
}

func NewSqliteStore(filename string) (engine.JobStore, error) {
	dsn := fmt.Sprintf("file:%s?cache=shared", filename)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	defaultClock := func() time.Time { return time.Now() }
	store := &sqliteStore{
		Filename: filename,
		DSN:      dsn,
		db:       db,
		sysClock: defaultClock,
	}
	return store, store.schemaInit()
}

func (s *sqliteStore) schemaInit() error {
	statement := `
	CREATE TABLE IF NOT EXISTS jobs_running (
		timestamp INTEGER,
		uuid BLOB(16),
		workflow TEXT,
		log BLOB
	);
	CREATE INDEX IF NOT EXISTS jr_time_ix on jobs_running (timestamp);
	CREATE INDEX IF NOT EXISTS jr_uuid_ix on jobs_running (uuid);

	CREATE TABLE IF NOT EXISTS jobs_completed (
		timestamp INTEGER,
		uuid BLOB(16) PRIMARY KEY,
		workflow TEXT,
		log BLOB
	) WITHOUT ROWID;
	CREATE INDEX IF NOT EXISTS jc_time_ix on jobs_completed (timestamp);
	CREATE INDEX IF NOT EXISTS jc_wrkf_ix on jobs_completed (workflow);
	`
	_, err := s.db.Exec(statement)
	return err
}

func (s *sqliteStore) Update(id uuid.UUID, workflow string, logs []*engine.LogEntry) error {
	var logData bytes.Buffer
	enc := gob.NewEncoder(&logData)
	err := enc.Encode(logs)
	if err != nil {
		return err
	}

	ts := s.sysClock().Unix()

	statement := `
	INSERT INTO jobs_running (timestamp, uuid, workflow, log) VALUES(?, ?, ?, ?)
	`
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err = s.db.Exec(statement, ts, id, workflow, logData.Bytes())

	return err
}

func decodeLogEntries(data []byte) ([]*engine.LogEntry, error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var logEntries []*engine.LogEntry
	err := dec.Decode(&logEntries)
	return logEntries, err
}

func (s *sqliteStore) getJobLogs(db *sql.DB, table string, id uuid.UUID) (*engine.JobLogInfo, error) {
	order := "timestamp ASC"
	if table == "jobs_running" {
		order += ", rowid ASC"
	}
	statement := fmt.Sprintf("SELECT log from %s WHERE uuid = ? ORDER BY %s", table, order)
	rows, err := db.Query(statement, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logsByStep := make(map[string]*engine.LogEntry)
	for rows.Next() {
		var data []byte
		err = rows.Scan(&data)
		if err != nil {
			return nil, err
		}
		logEntries, err := decodeLogEntries(data)
		if err != nil {
			return nil, err
		}
		for _, logEntry := range logEntries {
			logsByStep[logEntry.Step] = logEntry
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	if len(logsByStep) == 0 {
		return nil, ErrNoEntries
	}
	logs := make([]*engine.LogEntry, 0, len(logsByStep))
	for _, l := range logsByStep {
		logs = append(logs, l)
	}

	statement = fmt.Sprintf("SELECT DISTINCT workflow FROM %s WHERE uuid = ?", table)
	row := db.QueryRow(statement, id)
	var workflow string
	if err := row.Scan(&workflow); err != nil {
		return nil, err
	}
	jobInfo := &engine.JobLogInfo{ID: id, Workflow: workflow, Logs: logs}
	return jobInfo, nil
}

func (s *sqliteStore) GetRunningJobLogs(id uuid.UUID) (*engine.JobLogInfo, error) {
	db, err := sql.Open("sqlite3", s.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return s.getJobLogs(db, "jobs_running", id)
}

func (s *sqliteStore) GetCompletedJobLogs(id uuid.UUID) (*engine.JobLogInfo, error) {
	db, err := sql.Open("sqlite3", s.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return s.getJobLogs(db, "jobs_completed", id)
}

func (s *sqliteStore) OnJobDone(id uuid.UUID, workflow string, logs []*engine.LogEntry) error {
	var logData bytes.Buffer
	enc := gob.NewEncoder(&logData)
	err := enc.Encode(logs)
	if err != nil {
		return err
	}

	ts := s.sysClock().Unix()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	if _, err := tx.Exec("DELETE FROM jobs_running WHERE uuid = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO jobs_completed (timestamp, uuid, workflow, log) VALUES(?, ?, ?, ?)", ts, id, workflow, logData.Bytes()); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *sqliteStore) Recover() ([]engine.JobLogInfo, error) {
	db, err := sql.Open("sqlite3", s.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	statement := `
	SELECT uuid, workflow, log
	FROM jobs_running
	ORDER BY uuid ASC, timestamp ASC, rowid ASC
	`

	rows, err := db.Query(statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runningJobs []engine.JobLogInfo
	var current engine.JobLogInfo
	var logsByStep map[string]*engine.LogEntry

	flushCurrent := func() {
		if current.ID == uuid.Nil {
			return
		}
		current.Logs = make([]*engine.LogEntry, 0, len(logsByStep))
		for _, v := range logsByStep {
			current.Logs = append(current.Logs, v)
		}
		runningJobs = append(runningJobs, current)
	}

	for rows.Next() {
		var id uuid.UUID
		var workflow string
		var data []byte
		err = rows.Scan(&id, &workflow, &data)
		if err != nil {
			return nil, err
		}
		if current.ID != id {
			flushCurrent()
			current.ID = id
			current.Workflow = workflow
			logsByStep = make(map[string]*engine.LogEntry)
		}
		logEntries, err := decodeLogEntries(data)
		if err != nil {
			return nil, err
		}
		for _, logEntry := range logEntries {
			logsByStep[logEntry.Step] = logEntry
		}
	}
	flushCurrent()
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return runningJobs, nil
}

func (s *sqliteStore) ListCompletedJobs(workflow string, intervalMins int64) ([]uuid.UUID, error) {
	db, err := sql.Open("sqlite3", s.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var rows *sql.Rows

	if intervalMins > 0 {
		since := s.sysClock().Add(time.Duration(-intervalMins) * time.Minute).Unix()
		statement := `
		SELECT uuid
		FROM jobs_completed
		WHERE workflow = ? AND timestamp > ?
		ORDER BY timestamp DESC
		`
		rows, err = db.Query(statement, workflow, since)
		if err != nil {
			return nil, err
		}
	} else {
		statement := `
		SELECT uuid
		FROM jobs_completed
		WHERE workflow = ?
		ORDER BY timestamp DESC
		`
		rows, err = db.Query(statement, workflow)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	var jobIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		jobIDs = append(jobIDs, id)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return jobIDs, nil
}
