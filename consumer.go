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
	proxy proxy
}

type Message struct {
	Headers map[string]string
	Body    string
}

func NewConsumer(addr, group, topic, queue string) *Consumer {
	proxy := proxy{
		addr:   addr,
		group:  group,
		topic:  topic,
		queue:  queue,
		caller: defaultProxyCaller{},
	}
	return &Consumer{proxy}
}

func (q *Consumer) Consume(msgListener MsgListener, backoff int) (err error) {
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
		nr, err := q.consume(msgListener)
		if err != nil || nr == 0 {
			time.Sleep(time.Duration(backoff) * time.Second)
		}

	}
	return nil
}

const defaultBackoffPeriod = 8

func (q *Consumer) ConsumeCh(c chan<- Message) error {
	return q.Consume(defaultChMsgListener{c}, defaultBackoffPeriod)
}

type defaultChMsgListener struct {
	ch chan<- Message
}

func (d defaultChMsgListener) OnMessage(m Message) error {
	d.ch <- m
	return nil
}

func (q *Consumer) consume(msgListener MsgListener) (nr int, err error) {
	c, err := q.proxy.createConsumerInstance()
	if err != nil {
		log.Printf("ERROR - creating consumer instance: %s", err.Error())
		return 0, err
	}

	msgs, err := q.proxy.consumeMessages(c)
	if err != nil {
		log.Printf("ERROR - consuming messages: %s", err.Error())
		return 0, err
	}

	for _, m := range msgs {
		msgListener.OnMessage(m)
	}

	err = q.proxy.destroyConsumerInstance(c)
	if err != nil {
		log.Printf("ERROR - deleting consumer instance: %s", err.Error())
		return 0, err
	}
	return len(msgs), nil
}
