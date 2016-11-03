package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchConsumer(t *testing.T) {
	consumer := &DefaultBatchedQueueConsumer{baseQueueConsumer{config: QueueConfig{}, queue: defaultTestQueueCaller{}, consumer: consInstTest}, func(m []Message) {
		assert.Equal(t, msgsTest, m)
	}}

	msgs, err := consumer.consume()
	assert.Nil(t, err)
	assert.Equal(t, msgsTest, msgs)
}
