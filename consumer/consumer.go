package consumer

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

//This function is the entry point to using the gonsumer library
//It is a blocking function, it will return only when SIGINT or SIGTERM is received. If you don't want to block start it in a different goroutine.
func Start(config QueueConfig, handler func(m Message), client http.Client) {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	var wg sync.WaitGroup
	wg.Add(streamCount)
	for i := 0; i < streamCount; i++ {
		go func() {
			defer wg.Done()
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
			qc := NewQueueConsumer(config, handler, client)
			for {
				select {
				case <-ch:
					qc.shutdown()
					return
				default:
					qc.consumeAndHandleMessages()

				}
			}
		}()
	}
	wg.Wait()
}

type QueueConsumer interface {
	consumeAndHandleMessages()
	shutdown()
}

//DefaultQueueConsumer is the default implementation of the QueueConsumer interface.
//NOTE: DefaultQueueConsumer is not thread-safe!
type DefaultQueueConsumer struct {
	config   QueueConfig
	queue    queueCaller
	handler  func(m Message)
	consumer *consumer
}

type Message struct {
	Headers map[string]string
	Body    string
}

func NewQueueConsumer(config QueueConfig, handler func(m Message), client http.Client) QueueConsumer {
	offset := "largest"
	if len(config.Offset) > 0 {
		offset = config.Offset
	}
	queue := &defaultQueueCaller{
		addrs:  config.Addrs,
		group:  config.Group,
		topic:  config.Topic,
		offset: offset,
		caller: defaultHTTPCaller{config.Queue, config.AuthorizationKey, client},
	}
	return &DefaultQueueConsumer{config, queue, handler, nil}
}

func (c *DefaultQueueConsumer) consumeAndHandleMessages() {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			_, ok = r.(error)
			if !ok {
				log.Printf("Error: recovered from panic: %v", r)
			}
		}
	}()
	backoffPeriod := 8
	if c.config.BackoffPeriod > 0 {
		backoffPeriod = c.config.BackoffPeriod
	}

	msgs, err := c.consume()
	if err != nil || len(msgs) == 0 {
		time.Sleep(time.Duration(backoffPeriod) * time.Second)

	}
}

func (c *DefaultQueueConsumer) consume() ([]Message, error) {
	q := c.queue
	if c.consumer == nil {
		cInst, err := q.createConsumerInstance()
		if err != nil {
			log.Printf("ERROR - creating consumer instance: %s", err.Error())
			return nil, err
		}
		c.consumer = &cInst
	}
	msgs, err := q.consumeMessages(*c.consumer)
	if err != nil {
		log.Printf("ERROR - consuming messages: %s", err.Error())
		errD := q.destroyConsumerInstance(*c.consumer)
		if errD != nil {
			log.Printf("ERROR - deleting consumer instance: %s", errD.Error())
		}
		c.consumer = nil
		return nil, err
	}

	for _, msg := range msgs {
		c.handler(msg)
	}

	err = q.commitOffsets(*c.consumer)
	if err != nil {
		log.Printf("ERROR -  commiting offsets: %s", err.Error())
		errD := q.destroyConsumerInstance(*c.consumer)
		if errD != nil {
			log.Printf("ERROR - deleting consumer instance: %s", errD.Error())
		}
		c.consumer = nil
		return nil, err
	}

	return msgs, nil
}

func (c *DefaultQueueConsumer) shutdown() {
	if c.consumer != nil {
		err := c.queue.destroyConsumerInstance(*c.consumer)
		if err != nil {
			log.Printf("ERROR - deleting consumer instance: %s", err.Error())
		}
	}
}
