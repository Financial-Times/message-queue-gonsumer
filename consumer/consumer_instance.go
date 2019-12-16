package consumer

import (
	"net/http"
	"sync"
	"time"

	log "github.com/Financial-Times/go-logger/v2"
)

var offsetResetOptions = map[string]bool{
	"none":     true, // Not recommended for use because it throws exception to the consumer if no previous offset is found
	"earliest": true, // Not recommended for use bacause it will impact the memory usage of the proxy
	"latest":   true,
}

//NewConsumerInstance returns a new instance of consumerInstance
func NewConsumerInstance(config QueueConfig, handler func(m Message), client *http.Client, logger *log.UPPLogger) queueConsumer {
	offset := "latest"
	if offsetResetOptions[config.Offset] {
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
	return &consumerInstance{
		config:       config,
		queue:        queue,
		consumer:     nil,
		shutdownChan: make(chan bool, 1),
		processor:    splitMessageProcessor{handler},
		logger:       logger,
	}
}

type queueCaller interface {
	createConsumerInstance() (consumer, error)
	destroyConsumerInstance(c consumer) error
	subscribeConsumerInstance(c consumer) error
	destroyConsumerInstanceSubscription(c consumer) error
	consumeMessages(c consumer) ([]byte, error)
	commitOffsets(c consumer) error
	checkConnectivity() error
}

type messageProcessor interface {
	consume(messages ...Message)
}

//consumerInstance is the default implementation of the QueueConsumer interface.
//NOTE: consumerInstance is not thread-safe!
type consumerInstance struct {
	config       QueueConfig
	queue        queueCaller
	consumer     *consumer
	shutdownChan chan bool
	processor    messageProcessor
	logger       *log.UPPLogger
}

func (c *consumerInstance) consumeWhileActive() {
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

func (c *consumerInstance) consumeAndHandleMessages() {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				c.logger.WithError(err).Error("Recovered from panic")
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

func (c *consumerInstance) consume() ([]Message, error) {
	q := c.queue
	if c.consumer == nil {
		cInst, err := q.createConsumerInstance()
		if err != nil {
			c.logger.WithError(err).Error("Error creating consumer instance")
			return nil, err
		}
		c.consumer = &cInst

		err = q.subscribeConsumerInstance(*c.consumer)
		if err != nil {
			c.logger.WithError(err).Error("Error subscribing consumer instance to topic")

			c.shutdown()
			return nil, err
		}
	}

	res, err := q.consumeMessages(*c.consumer)
	if err != nil {
		c.logger.WithError(err).Error("Error consuming messages")

		c.shutdown()
		return nil, err
	}
	msgs, err := parseResponse(res, c.logger)
	if err != nil {
		c.logger.WithError(err).Error("Error parsing messages")

		c.shutdown()
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
					c.processor.consume(m)
				}

				rwWg.Done()
			}()
		}
		rwWg.Wait()

	} else {
		c.processor.consume(msgs...)
	}

	if c.config.AutoCommitEnable == false {
		err = q.commitOffsets(*c.consumer)
		if err != nil {
			c.logger.WithError(err).Error("Error commiting offsets")

			c.shutdown()
			return nil, err
		}
	}

	return msgs, nil
}

func (c *consumerInstance) shutdown() {
	if c.consumer != nil {
		err := c.queue.destroyConsumerInstanceSubscription(*c.consumer)
		if err != nil {
			c.logger.WithError(err).Error("Error deleting consumer instance subscription")
		}
		err = c.queue.destroyConsumerInstance(*c.consumer)
		if err != nil {
			c.logger.WithError(err).Error("Error deleting consumer instance")
		}

		c.consumer = nil
	}
}

func (c *consumerInstance) initiateShutdown() {
	c.shutdownChan <- true
}

func (c *consumerInstance) checkConnectivity() error {
	return c.queue.checkConnectivity()
}
