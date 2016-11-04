package consumer

import (
	"errors"
	"log"
	"net/http"
	"sync"
	"time"
)

type Consumer struct {
	streamCount int
	consumers   []QueueConsumer
}

func NewConsumer(config QueueConfig, handler func(m Message), client http.Client) Consumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	consumers := make([]QueueConsumer, streamCount)
	for i := 0; i < streamCount; i++ {
		consumers[i] = NewQueueConsumer(config, handler, client)
	}

	return Consumer{streamCount, consumers}
}

func NewBatchedConsumer(config QueueConfig, handler func(m []Message), client http.Client) Consumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}

	consumers := make([]QueueConsumer, streamCount)
	for i := 0; i < streamCount; i++ {
		consumers[i] = NewBatchedQueueConsumer(config, handler, client)
	}

	return Consumer{streamCount, consumers}
}

func NewAgeingConsumer(config QueueConfig, handler func(m Message), agingClient AgeingClient) Consumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	consumers := make([]QueueConsumer, streamCount)
	for i := 0; i < streamCount; i++ {
		consumers[i] = NewQueueConsumer(config, handler, agingClient.Client)
	}
	agingClient.StartAgeingProcess()

	return Consumer{streamCount, consumers}
}

//This function is the entry point to using the gonsumer library
//It is a blocking function, it will return only when Stop() is called. If you don't want to block start it in a different goroutine.
func (c *Consumer) Start() {
	var wg sync.WaitGroup
	wg.Add(c.streamCount)
	for _, consumer := range c.consumers {
		go func(consumer QueueConsumer) {
			defer wg.Done()
			consumer.consumeWhileActive()
		}(consumer)
	}
	wg.Wait()
}

func (c *Consumer) Stop() {
	for _, consumer := range c.consumers {
		consumer.initiateShutdown()
	}
}

type QueueConsumer interface {
	consumeWhileActive()
	initiateShutdown()
	shutdown()
}

//DefaultQueueConsumer is the default implementation of the QueueConsumer interface.
//NOTE: DefaultQueueConsumer is not thread-safe!
type DefaultQueueConsumer struct {
	baseQueueConsumer
	handler func(m Message)
}

type baseQueueConsumer struct {
	config       QueueConfig
	queue        queueCaller
	consumer     *consumer
	shutdownChan chan bool
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
		addrs:            config.Addrs,
		group:            config.Group,
		topic:            config.Topic,
		offset:           offset,
		autoCommitEnable: config.AutoCommitEnable,
		caller:           defaultHTTPCaller{config.Queue, config.AuthorizationKey, client},
	}
	return &DefaultQueueConsumer{baseQueueConsumer{config, queue, nil, make(chan bool, 1)}, handler}
}

func (c *baseQueueConsumer) consumeWhileActive() {
	for {
		select {
		case <-c.shutdownChan:
			c.shutdown()
			return
		default:
			c.consumeAndHandleMessages()
		}
	}

}

func (c *baseQueueConsumer) consumeAndHandleMessages() {
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

func (c *baseQueueConsumer) consume() ([]Message, error) {
	return nil, errors.New("Not implemented! Please use a DefaultQueueConsumer or DefaultBatchedQueueConsumer instead.")
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

	if c.config.ConcurrentProcessing == true {
		processors := 100
		if c.config.NoOfProcessors > 0 {
			processors = c.config.NoOfProcessors
		}
		rwWg := sync.WaitGroup{}
		ch := make(chan Message, 128)

		rwWg.Add(1)
		go func() {
			for _, msg := range msgs {
				ch <- msg
			}
			close(ch)
			rwWg.Done()
		}()

		for i := 0; i < processors; i++ {
			rwWg.Add(1)
			go func() {
				for m := range ch {
					c.handler(m)
				}

				rwWg.Done()
			}()
		}
		rwWg.Wait()

	} else {
		for _, msg := range msgs {
			c.handler(msg)
		}
	}

	if c.config.AutoCommitEnable == false {
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
	}

	return msgs, nil
}

func (c *baseQueueConsumer) shutdown() {
	if c.consumer != nil {
		err := c.queue.destroyConsumerInstance(*c.consumer)
		if err != nil {
			log.Printf("ERROR - deleting consumer instance: %s", err.Error())
		}
	}
}

func (c *baseQueueConsumer) initiateShutdown() {
	c.shutdownChan <- true
}
