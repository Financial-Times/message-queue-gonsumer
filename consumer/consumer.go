package consumer

import (
	"fmt"
	"log"
	"time"
)

//MsgListener contains the logic and strategy of consuming Messages.
//This must be implemented by the clients and passed as a param to the Consume(MsgListener, int) method.
type MsgListener interface {
	OnMessage(msg Message) error
}

type Consumer interface {
	Consume(msg MsgListener, backoff int) error
	ConsumeCh(ch chan<- Message) error
}

//Consumer is the high-level message consumer struct.
//Contains the queue config and has two methods to consume messages: an interface and a channel based one: Consume(MsgListener, int) && ConsumeCh(chan<- Message).
//One creates a *Consumer by Calling the NewConsumer(QueueConfig) function.
type DefaultConsumer struct {
	config QueueConfig
	queue  queueCaller
}

//Message is the higher-level representation of messages from the queue.
type Message struct {
	Headers map[string]string
	Body    string
}

//NewConsumer returns a pointer to a freshly created consumer.
//Read more @ queue-caller.go#QueueConfig.
func NewConsumer(config QueueConfig) Consumer {
	queue := defaultQueueCaller{
		addr:   config.Addr,
		group:  config.Group,
		topic:  config.Topic,
		caller: defaultHTTPCaller{config.Queue},
	}
	return &DefaultConsumer{config, queue}
}

//Consume method periodically checks for new messages determined by the backoff period.
//It accepts a MsgListener and on receiving new messages it passes them to the listener.
func (c *DefaultConsumer) Consume(msgListener MsgListener, backoff int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("Error: recovered from panic: %v", r)
			}
		}
	}()

	for {
		nr, err := c.consume(msgListener)
		if err != nil || nr == 0 {
			time.Sleep(time.Duration(backoff) * time.Second)
		}

	}
	return nil
}

const defaultBackoffPeriod = 8

//ConsumeCh method periodically checks for new messages determined by the a default backoff period.
//It accepts an inbound Message channel, and forwards new messages to this channel.
func (c *DefaultConsumer) ConsumeCh(ch chan<- Message) error {
	return c.Consume(defaultChMsgListener{ch}, defaultBackoffPeriod)
}

type defaultChMsgListener struct {
	ch chan<- Message
}

func (d defaultChMsgListener) OnMessage(m Message) error {
	d.ch <- m
	return nil
}

func (c *DefaultConsumer) consume(msgListener MsgListener) (nr int, err error) {
	cInst, err := c.queue.createConsumerInstance()
	if err != nil {
		log.Printf("ERROR - creating consumer instance: %s", err.Error())
		return 0, err
	}

	msgs, err := c.queue.consumeMessages(cInst)
	if err != nil {
		log.Printf("ERROR - consuming messages: %s", err.Error())
		return 0, err
	}

	for _, m := range msgs {
		msgListener.OnMessage(m)
	}

	err = c.queue.destroyConsumerInstance(cInst)
	if err != nil {
		log.Printf("ERROR - deleting consumer instance: %s", err.Error())
		return 0, err
	}
	return len(msgs), nil
}
