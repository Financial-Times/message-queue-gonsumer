Go implementation of https://github.com/Financial-Times/message-queue-consumer library

Usage:

`go get github.com/Financial-Times/message-queue-gonsumer/consumer`

`import github.com/Financial-Times/message-queue-gonsumer/consumer`


###Usage

The consumer API is like an iterator. First, the client creates a MessageIterator by calling:

 `consumer.NewIterator(QueueConf)`

Then whenever it is ready to consume new batch of messages, calls:

 `iterator.NextMessages()`

Which returns a slice of messages.


```go
conf := QueueConfig{
  Addr: "<addr>",
  Group: "<group>",
  Topic: "<topic>",
  Queue: "<required in co-co>",
  AuthorizationKey: "<required from AWS to UCS>",
}
myIterator := consumer.NewIterator(conf)

for {
  msgs, err := myIterator.NextMessages()
  //process msgs
}

```

###ToDo

1. More tests
2. Healthcheck
