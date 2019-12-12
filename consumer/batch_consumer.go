package consumer

import (
	"net/http"

	log "github.com/Financial-Times/go-logger/v2"
)

// NewBatchedQueueConsumer returns a new instance of a QueueConsumer that handles batches of messages
func NewBatchedQueueConsumer(config QueueConfig, handler func(m []Message), client *http.Client, logger *log.UPPLogger) queueConsumer {
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

	return &defaultQueueConsumer{
		config:       config,
		queue:        queue,
		consumer:     nil,
		shutdownChan: make(chan bool, 1),
		processor:    BatchedMessageProcessor{handler},
		logger:       logger,
	}
}

//BatchedMessageProcessor process messages in batches
type BatchedMessageProcessor struct {
	handler func(m []Message)
}

func (b BatchedMessageProcessor) consume(msgs ...Message) {
	if len(msgs) > 0 {
		b.handler(msgs)
	}
}
