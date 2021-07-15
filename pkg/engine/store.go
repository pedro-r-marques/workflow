package engine

import "github.com/google/uuid"

type ItemStore interface {
	Update(id uuid.UUID, workflow string, logs []*LogEntry) error
}
