package mbus

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"

	"github.com/pedro-r-marques/workflow/pkg/engine"
)

const defaultMessageQueueName = "workflow-manager"

var errNotConnected = errors.New("unable to send message to RabbitMQ server: not connected")

type rabbitMQSession struct {
	address         string
	vhost           string
	rcvHandler      func(correlationId string, body []byte)
	configQueueName string
	queueName       string
	sendChannel     *amqp.Channel
}

type rabbitMQBus struct {
	address         string
	configQueueName string
	rcvHandler      func(correlationId string, body []byte)
	vhosts          map[string]*rabbitMQSession
	mutex           sync.Mutex
}

func newRabbitMQSession(address, vhost, configQueueName string, rcvHandler func(correlationId string, body []byte)) *rabbitMQSession {
	return &rabbitMQSession{
		address:         address,
		vhost:           vhost,
		configQueueName: configQueueName,
		rcvHandler:      rcvHandler,
	}
}

func NewRabbitMQBus(address string, rcvHandler func(correlationId string, body []byte)) engine.MessageBus {
	return &rabbitMQBus{
		address:         address,
		configQueueName: defaultMessageQueueName,
		rcvHandler:      rcvHandler,
		vhosts:          make(map[string]*rabbitMQSession),
	}
}

func (b *rabbitMQBus) VHostInit(vhost string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, exists := b.vhosts[vhost]; !exists {
		session := newRabbitMQSession(b.address, vhost, b.configQueueName, b.rcvHandler)
		go session.Run()
		b.vhosts[vhost] = session
	}
}
func (b *rabbitMQBus) SendMsg(vhost string, qname string, correlationId string, msg map[string]json.RawMessage) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	session, exists := b.vhosts[vhost]
	if !exists {
		return fmt.Errorf("unknown vhost %s", vhost)
	}

	return session.SendMsg(qname, correlationId, msg)
}

func (s *rabbitMQSession) Run() {
	config := amqp.Config{
		Vhost: s.vhost,
	}
	for {
		connection, err := amqp.DialConfig(s.address, config)
		if err != nil {
			time.Sleep(30 * time.Second)
			continue
		}

		connErrChan := make(chan *amqp.Error)
		connection.NotifyClose(connErrChan)

		sendChannel, err := connection.Channel()
		if err != nil {
			connection.Close()
			continue
		}

		recvChannel, deliveryCh, err := s.recvChannelCreate(connection)
		if err != nil {
			sendChannel.Close()
			connection.Close()
			continue
		}

		sendErrChan := make(chan *amqp.Error)
		sendChannel.NotifyClose(sendErrChan)

		recvErrChan := make(chan *amqp.Error)
		recvChannel.NotifyClose(recvErrChan)

		s.sendChannel = sendChannel
		isConnected := true

		for isConnected {
			select {
			case m := <-deliveryCh:
				s.rcvHandler(m.CorrelationId, m.Body)

			case qerr := <-recvErrChan:
				log.Printf("amqp recv channel error: %v", *qerr)
				recvChannel, deliveryCh, err = s.recvChannelCreate(connection)
				if err == nil {
					recvChannel.NotifyClose(recvErrChan)
				} else {
					log.Printf("amqp recv channel reconnect: %v", err)
					sendChannel.Close()
					connection.Close()
					isConnected = false
				}

			case qerr := <-sendErrChan:
				log.Printf("amqp send channel error: %v", *qerr)
				sendChannel, err = connection.Channel()
				if err == nil {
					sendChannel.NotifyClose(sendErrChan)
				} else {
					log.Printf("amqp send channel reconnect: %v", err)
					recvChannel.Close()
					connection.Close()
					isConnected = false
				}

			case qerr := <-connErrChan:
				log.Printf("amqp connection error: %v", *qerr)
				isConnected = false
			}
		}
		s.sendChannel = nil
	}
}

func (s *rabbitMQSession) recvChannelCreate(connection *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error) {
	recvChannel, err := connection.Channel()
	if err != nil {
		return nil, nil, err
	}

	if err := s.configRecvQueue(recvChannel); err != nil {
		return nil, nil, err
	}

	ch, err := recvChannel.Consume(s.queueName, "", true, true, false, false, nil)
	return recvChannel, ch, err
}

func (s *rabbitMQSession) configRecvQueue(ch *amqp.Channel) error {
	q, err := ch.QueueDeclare(
		s.configQueueName,
		false, // durable
		false, // autoDelete
		true,  // exclusive
		false, // noWait
		nil)
	s.queueName = q.Name
	return err
}

func (s *rabbitMQSession) SendMsg(qname string, correlationId string, data map[string]json.RawMessage) error {
	if s.sendChannel == nil {
		return errNotConnected
	}
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("RabbitMQ send: %w", err)
	}
	msg := amqp.Publishing{
		CorrelationId: correlationId,
		ContentType:   "application/json",
		ReplyTo:       s.queueName,
		DeliveryMode:  amqp.Persistent,
		Body:          body,
	}
	return s.sendChannel.Publish(
		"",    // exchange
		qname, // routing-key
		false, // mandatory
		false, // immediate
		msg)
}
