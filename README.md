Go implementation of https://github.com/Financial-Times/message-queue-consumer library

Usage:

`go get https://github.com/Financial-Times/message-queue-gonsumer/consumer`

`import https://github.com/Financial-Times/message-queue-gonsumer/consumer`


There are two ways to use the API:
1. through consumer.Consume(MsgListener, backoffPeriod), where clients must provide an implementation for the MsgListener
2. through a channel based consume.ConsumeCh(chan<- Message), where clients receive messages from the provided channel


```go
//implement consumer.MsgListener interface
type listenerImpl struct {}
func (l listenerImpl) OnMessage(m Message) error {
  //... process msg
}

conf := QueueConfig{
  Addr: "<addr>",
  Group: "<group>",
  Topic: "<topic>",
  Queue: "<required in co-co>",
}
myConsumer := consumer.NewConsumer(conf)

listener := listenerImpl{}
myConsumer.Consume(listener}, 8)```

###ToDo

1. More tests
2. Healthcheck
