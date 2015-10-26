package consumer

import (
	"errors"
	"reflect"
	"testing"
)

func TestConsume(t *testing.T) {
	var tests = []struct {
		iterator *DefaultIterator
		expMsgs  []Message
		expErr   error
		expCons  *consumer //consumerInstance of DefaultIterator
	}{
		{
			&DefaultIterator{queue: default_queueCaller{}, consumer: consInst_test},
			msgs_test,
			nil,
			consInst_test,
		},
		{
			&DefaultIterator{queue: default_queueCaller{}},
			msgs_test,
			nil,
			consInst_test,
		},
		{
			&DefaultIterator{queue: consumeMsgError_queueCaller{}, consumer: consInst_test},
			nil,
			errors.New("Error while consuming"),
			nil,
		},
	}

	for _, test := range tests {
		actMsgs, actErr := test.iterator.consume()
		if !reflect.DeepEqual(actMsgs, test.expMsgs) || !reflect.DeepEqual(test.iterator.consumer, test.expCons) || !reflect.DeepEqual(test.expErr, actErr) {
			t.Errorf("Expected: nr: %d, error: %v, iterator: %v\nActual: nr: %d, error: %v iterator: %v.",
				test.expMsgs, test.expErr, test.expCons, actMsgs, actErr, test.iterator.consumer)
		}
	}
}

var consInst_test = &consumer{"/queue/consumergroup/instance-d", "/instance-id"}
var msgs_test = []Message{Message{nil, "body"}, Message{map[string]string{"Message-Id": "0000-1111-0000-abcd"}, "[]"}}

//test queueCaller implementations

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
	return msgs_test, nil
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
