package engine

import (
	"encoding/json"
)

type MessageBus interface {
	VHostInit(vhost string)
	SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error
}
