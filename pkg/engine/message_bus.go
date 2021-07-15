package engine

import (
	"encoding/json"
)

type MessageBus interface {
	SendMsg(vhost string, qname string, msg map[string]json.RawMessage) error
}
