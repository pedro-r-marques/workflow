package store

import (
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	"github.com/pedro-r-marques/workflow/pkg/engine"
)

type sqlliteItemStore struct {
	Filename string
}

func NewSqliteItemStore(filename string) engine.ItemStore {
	return &sqlliteItemStore{Filename: filename}
}

func (s *sqlliteItemStore) Update(id uuid.UUID, workflow string, logs []*engine.LogEntry) error {
	return nil
}
