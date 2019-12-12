package consumer

import (
	"testing"

	log "github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
)

func TestBatchConsumer(t *testing.T) {
	consumer := &consumerInstance{
		config:   QueueConfig{},
		queue:    defaultTestQueueCaller{},
		consumer: consInstTest, processor: BatchedMessageProcessor{func(m []Message) {
			assert.Equal(t, msgsTest, m)
		}},
		logger: log.NewUPPLogger("Test", "FATAL"),
	}

	msgs, err := consumer.consume()
	assert.Nil(t, err)
	assert.Equal(t, msgsTest, msgs)
}
