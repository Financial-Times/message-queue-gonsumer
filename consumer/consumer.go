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

// NewConsumer returns a new instance of a Consumer
func NewConsumer(config QueueConfig, handler func(m Message), client *http.Client, logger *log.UPPLogger) MessageConsumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	instanceHandlers := make([]instanceHandler, streamCount)
	for i := 0; i < streamCount; i++ {
		instanceHandlers[i] = NewConsumerInstance(config, handler, client, logger)
	}

	return &Consumer{streamCount, instanceHandlers}
}

// NewBatchedConsumer returns a Consumer to manage batches of messages
func NewBatchedConsumer(config QueueConfig, handler func(m []Message), client *http.Client, logger *log.UPPLogger) MessageConsumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}

	instanceHandlers := make([]instanceHandler, streamCount)
	for i := 0; i < streamCount; i++ {
		instanceHandlers[i] = NewBatchedConsumerInstance(config, handler, client, logger)
	}

	return &Consumer{streamCount, instanceHandlers}
}

// NewAgeingConsumer returns a new instance of a Consumer with an AgeingClient
func NewAgeingConsumer(config QueueConfig, handler func(m Message), agingClient AgeingClient, logger *log.UPPLogger) MessageConsumer {
	streamCount := 1
	if config.StreamCount > 0 {
		streamCount = config.StreamCount
	}
	instanceHandlers := make([]instanceHandler, streamCount)
	for i := 0; i < streamCount; i++ {
		instanceHandlers[i] = NewConsumerInstance(config, handler, agingClient.Client, logger)
	}
	agingClient.StartAgeingProcess()

	return &Consumer{streamCount, instanceHandlers}
}

type instanceHandler interface {
	consumeWhileActive()
	initiateShutdown()
	shutdown()
	checkConnectivity() error
}

// Consumer provides methods to consume messages from a kafka proxy
type Consumer struct {
	streamCount      int
	instanceHandlers []instanceHandler
}

//Start is a method that triggers the consumption of messages from the queue
//Start is a blocking methode, it will return only when Stop() is called. If you don't want to block start it in a different goroutine.
func (c *Consumer) Start() {
	var wg sync.WaitGroup
	wg.Add(c.streamCount)
	for _, ih := range c.instanceHandlers {
		go func(ih instanceHandler) {
			defer wg.Done()
			ih.consumeWhileActive()
		}(ih)
	}
	wg.Wait()
}

//Stop is a methode to stop the consumer
func (c *Consumer) Stop() {
	for _, ih := range c.instanceHandlers {
		ih.initiateShutdown()
	}
}

//ConnectivityCheck returns the connection status with the kafka proxy
func (c *Consumer) ConnectivityCheck() (string, error) {
	errMsg := ""
	for _, ih := range c.instanceHandlers {
		if err := ih.checkConnectivity(); err != nil {
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
