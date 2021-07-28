package store

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
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
}

func NewSqliteStore(filename string) (engine.JobStore, error) {
	dsn := fmt.Sprintf("file://%s?cache=shared", filename)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Panic(err)
	}
	store := &sqliteStore{Filename: filename, DSN: dsn, db: db}
	return store, store.schemaInit()
}

func (s *sqliteStore) schemaInit() error {
	statement := `
	CREATE TABLE IF NOT EXISTS jobs_running (timestamp INTEGER, uuid BLOB(16), workflow TEXT, log BLOB);
	CREATE INDEX IF NOT EXISTS jr_time_ix on jobs_running (timestamp);
	CREATE INDEX IF NOT EXISTS jr_uuid_ix on jobs_running (uuid);

	CREATE TABLE IF NOT EXISTS jobs_completed (timestamp INTEGER, uuid BLOB(16) PRIMARY KEY, workflow TEXT, log BLOB) WITHOUT ROWID;
	CREATE INDEX IF NOT EXISTS jc_time_ix on jobs_running (timestamp);
	CREATE INDEX IF NOT EXISTS jc_wrkf_ix on jobs_running (workflow);
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

	ts := time.Now().Unix()

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

func (s *sqliteStore) getJobLogs(db *sql.DB, table string, id uuid.UUID) ([]*engine.LogEntry, error) {
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
	return logs, nil
}

func (s *sqliteStore) GetRunningJobLogs(id uuid.UUID) ([]*engine.LogEntry, error) {
	db, err := sql.Open("sqlite3", s.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return s.getJobLogs(db, "jobs_running", id)
}

func (s *sqliteStore) GetCompletedJobLogs(id uuid.UUID) ([]*engine.LogEntry, error) {
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

	ts := time.Now().Unix()

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
