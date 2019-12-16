package consumer

import (
	"errors"
	"net/http"
	"sync"

	log "github.com/Financial-Times/go-logger/v2"
)

// MessageConsumer is a high level generic interface for consumers.
//
// Start triggers the consumption of messages.
//
// Stop method stops the consumption of messages.
//
// ConnectivityCheck implements the logic to check the current
// connectivity to the queue.
// The method should return a message about the status of the connection and
// an error in case of connectivity failure.
type MessageConsumer interface {
	Start()
	Stop()
	ConnectivityCheck() (string, error)
}

type queueConsumer interface {
	consumeWhileActive()
	initiateShutdown()
	shutdown()
	checkConnectivity() error
}

// NewConsumer returns a new instance of a Consumer
func NewConsumer(config QueueConfig, handler func(m Message), client *http.Client, logger *log.UPPLogger) MessageConsumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	consumers := make([]queueConsumer, streamCount)
	for i := 0; i < streamCount; i++ {
		consumers[i] = NewConsumerInstance(config, handler, client, logger)
	}

	return &Consumer{streamCount, consumers}
}

// NewBatchedConsumer returns a Consumer to manage batches of messages
func NewBatchedConsumer(config QueueConfig, handler func(m []Message), client *http.Client, logger *log.UPPLogger) MessageConsumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}

	consumers := make([]queueConsumer, streamCount)
	for i := 0; i < streamCount; i++ {
		consumers[i] = NewBatchedConsumerInstance(config, handler, client, logger)
	}

	return &Consumer{streamCount, consumers}
}

// NewAgeingConsumer returns a new instance of a Consumer with an AgeingClient
func NewAgeingConsumer(config QueueConfig, handler func(m Message), agingClient AgeingClient, logger *log.UPPLogger) MessageConsumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	consumers := make([]queueConsumer, streamCount)
	for i := 0; i < streamCount; i++ {
		consumers[i] = NewConsumerInstance(config, handler, agingClient.Client, logger)
	}
	agingClient.StartAgeingProcess()

	return &Consumer{streamCount, consumers}
}

// Consumer provides methods to consume messages from a kafka proxy
type Consumer struct {
	streamCount int
	consumers   []queueConsumer
}

//Start is a method that triggers the consumption of messages from the queue
//Start is a blocking methode, it will return only when Stop() is called. If you don't want to block start it in a different goroutine.
func (c *Consumer) Start() {
	var wg sync.WaitGroup
	wg.Add(c.streamCount)
	for _, consumer := range c.consumers {
		go func(consumer queueConsumer) {
			defer wg.Done()
			consumer.consumeWhileActive()
		}(consumer)
	}
	wg.Wait()
}

//Stop is a methode to stop the consumer
func (c *Consumer) Stop() {
	for _, consumer := range c.consumers {
		consumer.initiateShutdown()
	}
}

//ConnectivityCheck returns the connection status with the kafka proxy
func (c *Consumer) ConnectivityCheck() (string, error) {
	errMsg := ""
	for _, consumer := range c.consumers {
		if err := consumer.checkConnectivity(); err != nil {
			errMsg = errMsg + err.Error()
		}
	}
	if errMsg == "" {
		return "Connectivity to consumer proxies is OK.", nil
	}

	return "Error connecting to consumer proxies", errors.New(errMsg)
}

//Message defines the consumed messages
type Message struct {
	Headers map[string]string
	Body    string
}

//SplitMessageProcessor processes messages one by one
type splitMessageProcessor struct {
	handler func(m Message)
}

func (p splitMessageProcessor) consume(msgs ...Message) {
	for _, msg := range msgs {
		p.handler(msg)
	}
}
