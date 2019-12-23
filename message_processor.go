package consumer

// Message defines the consumed messages
type Message struct {
	Headers map[string]string
	Body    string
}

// splitMessageProcessor processes messages one by one
type splitMessageProcessor struct {
	handler func(m Message)
}

func (p splitMessageProcessor) consume(msgs ...Message) {
	for _, msg := range msgs {
		p.handler(msg)
	}
}

// batchedMessageProcessor process messages in batches
type batchedMessageProcessor struct {
	handler func(m []Message)
}

func (b batchedMessageProcessor) consume(msgs ...Message) {
	if len(msgs) > 0 {
		b.handler(msgs)
	}
}
