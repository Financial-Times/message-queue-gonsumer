package consumer

import (
	"fmt"
	"log"
	"time"
)

type MsgListener interface {
	OnMessage(msg Message) error
}

type Consumer struct {
	Queue QueueConfig
}

type Message struct {
	Headers map[string]string
	Body    string
}

func NewConsumer(config QueueConfig) *Consumer {
	if config.caller == nil {
		config.caller = defaultProxyCaller{}
	}
	return &Consumer{config}
}

func (c *Consumer) Consume(msgListener MsgListener, backoff int) (err error) {
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

func (c *Consumer) ConsumeCh(ch chan<- Message) error {
	return c.Consume(defaultChMsgListener{ch}, defaultBackoffPeriod)
}

type defaultChMsgListener struct {
	ch chan<- Message
}

func (d defaultChMsgListener) OnMessage(m Message) error {
	d.ch <- m
	return nil
}

func (c *Consumer) consume(msgListener MsgListener) (nr int, err error) {
	cInst, err := c.Queue.createConsumerInstance()
	if err != nil {
		log.Printf("ERROR - creating consumer instance: %s", err.Error())
		return 0, err
	}

	msgs, err := c.Queue.consumeMessages(cInst)
	if err != nil {
		log.Printf("ERROR - consuming messages: %s", err.Error())
		return 0, err
	}

	for _, m := range msgs {
		msgListener.OnMessage(m)
	}

	err = c.Queue.destroyConsumerInstance(cInst)
	if err != nil {
		log.Printf("ERROR - deleting consumer instance: %s", err.Error())
		return 0, err
	}
	return len(msgs), nil
}
