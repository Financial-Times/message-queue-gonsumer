#message-queue-gonsumer

[![GoDoc](https://godoc.org/github.com/Financial-Times/message-queue-gonsumer/consumer?status.svg)](https://godoc.org/github.com/Financial-Times/message-queue-gonsumer/consumer)

[![Circle CI](https://circleci.com/gh/Financial-Times/message-queue-gonsumer.svg?style=shield)](https://circleci.com/gh/Financial-Times/message-queue-gonsumer/tree/master)

Go implementation of https://github.com/Financial-Times/message-queue-consumer library

###Usage

`go get github.com/Financial-Times/message-queue-gonsumer/consumer`

`import github.com/Financial-Times/message-queue-gonsumer/consumer`

The consumer API is used by calling:

 `consumer.NewConsumer(QueueConf, func (message Message), httpClient).Start()`

According the QueueConfig it will start consuming messages on one or more streams and call the passed in function for every message. Make sure the function you pass in is thread safe.

```go
conf := QueueConfig{
  Addr: "<addr>",
  Group: "<group>",
  Topic: "<topic>",
  Queue: "<required in co-co>",
  Offset: "<set to `smallest` otherwise the default `largest` will be considered>",
  BackoffPeriod: "<Period in seconds to back off if error occured or queue is empty>",
  StreamCount: "<Number of goroutines used to consume/process messages. Defaults to 1>",
  ConcurrentProcessing: <true|false Whether messages can be processed concurrently or not>,
  AuthorizationKey: "<required from AWS to UCS>",
}
c := queueConsumer.NewConsumer(conf, func(m queueConsumer.Message) { //process message in a thread safe manner }, http.Client{})
go c.Start()
c.Stop()
```

###ToDo

1. More tests
2. Healthcheck
