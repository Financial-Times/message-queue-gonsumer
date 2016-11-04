package consumer

import (
	"errors"
	"log"
	"net/http"
)

type DefaultBatchedQueueConsumer struct {
	baseQueueConsumer
	handler func(m []Message)
}

func NewBatchedQueueConsumer(config QueueConfig, handler func(m []Message), client http.Client) QueueConsumer {
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
	return &DefaultBatchedQueueConsumer{baseQueueConsumer{config, queue, nil, make(chan bool, 1)}, handler}
}

func (c *DefaultBatchedQueueConsumer) consume() ([]Message, error) {
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

	if c.config.ConcurrentProcessing {
		return nil, errors.New("This batched queue consumer does not support concurrent processing! Please set ConcurrentProcessing to false.")
	}

	c.handler(msgs)

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
