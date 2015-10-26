package consumer

import (
	"errors"
	"reflect"
	"testing"
)

func TestConsume(t *testing.T) {

	var tests = []struct {
		consumer *DefaultConsumer
		expErr   error
		expNr    int
		expCons  *consumer //consumerInstance on consumer
	}{
		{
			&DefaultConsumer{queue: default_queueCaller{}, consumer: consInst_test},
			nil,
			5,
			consInst_test,
		},
		{
			&DefaultConsumer{queue: default_queueCaller{}},
			nil,
			5,
			consInst_test,
		},
		{
			&DefaultConsumer{queue: consumeMsgError_queueCaller{}, consumer: consInst_test},
			errors.New("Error while consuming"),
			0,
			nil,
		},
	}

	for _, test := range tests {
		actNr, actErr := test.consumer.consume(msgListener_test)
		if actNr != test.expNr || !reflect.DeepEqual(test.consumer.consumer, test.expCons) ||
			(actErr != test.expErr && actErr.Error() != test.expErr.Error()) {
			t.Errorf("Expected: nr: %d, error: %v, consumer: %v\nActual: nr: %d, error: %v consumer: %v.", test.expNr, test.expErr, test.expCons, actNr, actErr, test.consumer.consumer)
		}
	}
}

type testMsgListener struct{}

func (list testMsgListener) OnMessage(m Message) error {
	return nil
}

var msgListener_test = testMsgListener{}
var consInst_test = &consumer{"/queue/consumergroup/instance-d", "/instance-id"}

//test queue caller implementations

//default happy-case behaviour
type default_queueCaller struct{}

func (qc default_queueCaller) createConsumerInstance() (consumer, error) {
	return *consInst_test, nil
}

func (qc default_queueCaller) destroyConsumerInstance(cInst consumer) error {
	if len(cInst.BaseURI) == 0 && len(cInst.InstanceID) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

func (qc default_queueCaller) consumeMessages(cInst consumer) ([]Message, error) {
	if len(cInst.BaseURI) == 0 && len(cInst.InstanceID) == 0 {
		return nil, errors.New("consumer instance is nil")
	}
	return make([]Message, 5), nil
}

//return error on consume and destroy
type consumeMsgError_queueCaller struct {
	qc default_queueCaller
}

func (qc consumeMsgError_queueCaller) createConsumerInstance() (consumer, error) {
	return qc.qc.createConsumerInstance()
}

func (qc consumeMsgError_queueCaller) destroyConsumerInstance(cInst consumer) error {
	return errors.New("Error while destroying")
}

func (qc consumeMsgError_queueCaller) consumeMessages(cInst consumer) ([]Message, error) {
	return nil, errors.New("Error while consuming")
}
