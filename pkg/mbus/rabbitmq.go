package mbus

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"

	"github.com/pedro-r-marques/workflow/pkg/engine"
)

const defaultMessageQueueName = "workflow-manager"

var errNotConnected = errors.New("unable to send message to RabbitMQ server: not connected")

type rabbitMQSession struct {
	address         string
	vhost           string
	rcvHandler      func(correlationId string, body []byte) error
	configQueueName string
	queueName       string
	sendChannel     *amqp.Channel
}

type rabbitMQBus struct {
	address         string
	configQueueName string
	rcvHandler      func(correlationId string, body []byte) error
	vhosts          map[string]*rabbitMQSession
	mutex           sync.Mutex
}

func newRabbitMQSession(address, vhost, configQueueName string, rcvHandler func(correlationId string, body []byte) error) *rabbitMQSession {
	return &rabbitMQSession{
		address:         address,
		vhost:           vhost,
		configQueueName: configQueueName,
		rcvHandler:      rcvHandler,
	}
}

func NewRabbitMQBus(address string) engine.MessageBus {
	return &rabbitMQBus{
		address:         address,
		configQueueName: defaultMessageQueueName,
		vhosts:          make(map[string]*rabbitMQSession),
	}
}

func (b *rabbitMQBus) SetHandler(rcvHandler engine.MessageBusRecvHandler) {
	b.rcvHandler = rcvHandler
}

func (b *rabbitMQBus) VHostInit(vhost string) error {
	if b.rcvHandler == nil {
		return fmt.Errorf("handler must be set before vhost initialization")
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, exists := b.vhosts[vhost]; !exists {
		session := newRabbitMQSession(b.address, vhost, b.configQueueName, b.rcvHandler)
		go session.Run()
		b.vhosts[vhost] = session
	}
	return nil
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
				log.Debug().
					Str("appId", m.AppId).
					Str("correlationId", m.CorrelationId).
					RawJSON("payload", m.Body).
					Msg("recv")

				s.rcvHandler(m.CorrelationId, m.Body)

			case qerr := <-recvErrChan:
				log.Error().Msgf("amqp recv channel error: %v", *qerr)
				recvChannel, deliveryCh, err = s.recvChannelCreate(connection)
				if err == nil {
					recvChannel.NotifyClose(recvErrChan)
				} else {
					log.Error().Msgf("amqp recv channel reconnect: %v", err)
					sendChannel.Close()
					connection.Close()
					isConnected = false
				}

			case qerr := <-sendErrChan:
				log.Error().Msgf("amqp send channel error: %v", *qerr)
				sendChannel, err = connection.Channel()
				if err == nil {
					sendChannel.NotifyClose(sendErrChan)
				} else {
					log.Error().Msgf("amqp send channel reconnect: %v", err)
					recvChannel.Close()
					connection.Close()
					isConnected = false
				}

			case qerr := <-connErrChan:
				log.Error().Msgf("amqp connection error: %v", *qerr)
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
		true,  // durable
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

	log.Debug().
		Str("qname", qname).
		Str("correlationId", correlationId).
		RawJSON("payload", body).
		Msg("send")

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
