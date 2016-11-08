package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchConsumer(t *testing.T) {
	consumer := &DefaultQueueConsumer{config: QueueConfig{}, queue: defaultTestQueueCaller{}, consumer: consInstTest, processor: BatchedMessageProcessor{func(m []Message) {
		assert.Equal(t, msgsTest, m)
	}}}

	msgs, err := consumer.consume()
	assert.Nil(t, err)
	assert.Equal(t, msgsTest, msgs)
}
