Go implementation of https://github.com/Financial-Times/message-queue-consumer library

Usage:

`go get github.com/Financial-Times/message-queue-gonsumer/consumer`

`import github.com/Financial-Times/message-queue-gonsumer/consumer`


There are two ways to use the API:

1. `consumer.Consume(MsgListener, backoffPeriod)`: clients must provide an implementation for the MsgListener
2. `consumer.ConsumeCh(chan<- Message)`: clients receive messages from the provided channel


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
myConsumer.Consume(listener}, 8)
```

###ToDo

1. More tests
2. Healthcheck
