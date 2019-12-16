package consumer

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	log "github.com/Financial-Times/go-logger/v2"
)

func TestConsume(t *testing.T) {
	logger := log.NewUPPLogger("Test", "FATAL")

	var tests = []struct {
		consumer *consumerInstance
		expMsgs  []Message
		expErr   error
		expCons  *consumerInstanceURI //DefaultIterator's consumerInstance
	}{
		{
			consumer: &consumerInstance{
				config: QueueConfig{}, queue: defaultTestQueueCaller{}, consumer: consInstTest,
				processor: splitMessageProcessor{func(m Message) {}}, logger: logger},
			expMsgs: msgsTest,
			expCons: consInstTest,
		},
		{
			consumer: &consumerInstance{
				config: QueueConfig{}, queue: defaultTestQueueCaller{},
				processor: splitMessageProcessor{func(m Message) {}}, logger: logger},
			expMsgs: msgsTest,
			expCons: consInstTest,
		},
		{
			consumer: &consumerInstance{
				config: QueueConfig{}, queue: consumeMsgErrorQueueCaller{}, consumer: consInstTest,
				processor: splitMessageProcessor{func(m Message) {}}, logger: logger},
			expErr: errors.New("error while consuming"),
		},
	}

	for _, test := range tests {
		actMsgs, actErr := test.consumer.consume()
		if !reflect.DeepEqual(actMsgs, test.expMsgs) || !reflect.DeepEqual(test.consumer.consumer, test.expCons) || !reflect.DeepEqual(test.expErr, actErr) {
			t.Errorf("Expected: msgs: %v, error: %v, consumer: %v\nActual: msgs: %v, error: %v consumer: %v.",
				test.expMsgs, test.expErr, test.expCons, actMsgs, actErr, test.consumer.consumer)
		}
	}
}

func TestConsumeAndHandleMessagesRecoversFromPanic(t *testing.T) {
	c := consumerInstance{config: QueueConfig{BackoffPeriod: 1}, queue: consumeMsgPanicQueueCaller{}, processor: splitMessageProcessor{func(m Message) {}}}
	c.consumeAndHandleMessages()
}

func TestConsumeWhileActiveTerminates(t *testing.T) {
	sdChan := make(chan bool)
	c := consumerInstance{config: QueueConfig{}, queue: defaultTestQueueCaller{}, shutdownChan: sdChan, processor: splitMessageProcessor{func(m Message) {}}}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		c.consumeWhileActive()
		wg.Done()
	}()
	sdChan <- true
	wg.Wait()
}

func TestStartStop(t *testing.T) {
	consumers := make([]instanceHandler, 2)
	for i := 0; i < 2; i++ {
		consumers[i] = &consumerInstance{config: QueueConfig{}, queue: defaultTestQueueCaller{}, shutdownChan: make(chan bool), processor: splitMessageProcessor{func(m Message) {}}}
	}
	c := Consumer{2, consumers}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		c.Start()
		wg.Done()
	}()
	c.Stop()
	wg.Wait()
}

var consInstTest = &consumerInstanceURI{"/queue/consumergroup/instance-d"}
var msgsTestByteA = []byte(`[{"value":"RlRNU0cvMS4wCgpib2R5Cg==","partition":0,"offset":0},{"value":"TWVzc2FnZS1JZDogMDAwMC0xMTExLTAwMDAtYWJjZAoKW10K","partition":0,"offset":1}]`)
var msgsTest = []Message{{nil, "body"}, {map[string]string{"Message-Id": "0000-1111-0000-abcd"}, "[]"}}

//test queueCaller implementations

//default happy-case behaviour
type defaultTestQueueCaller struct {
}

func (qc defaultTestQueueCaller) createConsumerInstance() (consumerInstanceURI, error) {
	return *consInstTest, nil
}

func (qc defaultTestQueueCaller) destroyConsumerInstance(cInst consumerInstanceURI) error {
	if len(cInst.BaseURI) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

func (qc defaultTestQueueCaller) subscribeConsumerInstance(cInst consumerInstanceURI) error {
	if len(cInst.BaseURI) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

func (qc defaultTestQueueCaller) destroyConsumerInstanceSubscription(cInst consumerInstanceURI) error {
	if len(cInst.BaseURI) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

func (qc defaultTestQueueCaller) consumeMessages(cInst consumerInstanceURI) ([]byte, error) {
	if len(cInst.BaseURI) == 0 {
		return nil, errors.New("consumer instance is nil")
	}
	return msgsTestByteA, nil
}

func (qc defaultTestQueueCaller) commitOffsets(cInst consumerInstanceURI) error {
	if len(cInst.BaseURI) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

func (qc defaultTestQueueCaller) checkConnectivity() error {
	return nil
}

//return error on consume and destroy
type consumeMsgErrorQueueCaller struct {
	qc defaultTestQueueCaller
}

func (qc consumeMsgErrorQueueCaller) createConsumerInstance() (consumerInstanceURI, error) {
	return qc.qc.createConsumerInstance()
}

func (qc consumeMsgErrorQueueCaller) destroyConsumerInstance(cInst consumerInstanceURI) error {
	return errors.New("error while destroying")
}

func (qc consumeMsgErrorQueueCaller) subscribeConsumerInstance(cInst consumerInstanceURI) error {
	return nil
}

func (qc consumeMsgErrorQueueCaller) destroyConsumerInstanceSubscription(cInst consumerInstanceURI) error {
	return errors.New("error while destroying subscription")
}

func (qc consumeMsgErrorQueueCaller) consumeMessages(cInst consumerInstanceURI) ([]byte, error) {
	return nil, errors.New("error while consuming")
}

func (qc consumeMsgErrorQueueCaller) commitOffsets(cInst consumerInstanceURI) error {
	return errors.New("error while committing offsets")
}

func (qc consumeMsgErrorQueueCaller) checkConnectivity() error {
	return errors.New("connectivity error")
}

type consumeMsgPanicQueueCaller struct {
	qc defaultTestQueueCaller
}

func (qc consumeMsgPanicQueueCaller) createConsumerInstance() (consumerInstanceURI, error) {
	return qc.qc.createConsumerInstance()
}

func (qc consumeMsgPanicQueueCaller) destroyConsumerInstance(cInst consumerInstanceURI) error {
	panic("Panic")
}

func (qc consumeMsgPanicQueueCaller) subscribeConsumerInstance(cInst consumerInstanceURI) error {
	return nil
}

func (qc consumeMsgPanicQueueCaller) destroyConsumerInstanceSubscription(cInst consumerInstanceURI) error {
	return errors.New("error while destroying subscription")
}

func (qc consumeMsgPanicQueueCaller) consumeMessages(cInst consumerInstanceURI) ([]byte, error) {
	return nil, errors.New("error while consuming")
}

func (qc consumeMsgPanicQueueCaller) commitOffsets(cInst consumerInstanceURI) error {
	return errors.New("error while committing offsets")
}

func (qc consumeMsgPanicQueueCaller) checkConnectivity() error {
	return errors.New("connectivity error")
}
