package mbus

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQRecv(t *testing.T) {
	amqp_url := os.Getenv("AMQP_SERVER")
	if amqp_url == "" {
		t.Skip("env variable AMQP_SERVER not defined")
	}

	var mId string
	handler := func(correlationId string, body []byte) {
		mId = correlationId
	}

	mbus := NewRabbitMQBus(amqp_url, handler)
	mbus.(*rabbitMQBus).configQueueName = "testing"
	mbus.VHostInit("")

	var msg map[string]json.RawMessage
	var err error
	for i := 0; i < 5; i++ {
		err = mbus.SendMsg("", "testing", "x", msg)
		if err == errNotConnected {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	assert.NoError(t, err)
	for i := 0; i < 5; i++ {
		if mId == "" {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	assert.Equal(t, "x", mId)
}
