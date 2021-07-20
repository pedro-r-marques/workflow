package engine

import (
	"encoding/json"
)

type MessageBusRecvHandler func(correlationId string, body []byte) error

type MessageBus interface {
	SetHandler(MessageBusRecvHandler)
	VHostInit(vhost string) error
	SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error
}
