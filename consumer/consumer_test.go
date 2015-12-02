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
		expCons  *consumer //DefaultIterator's consumerInstance
	}{
		{
			&DefaultIterator{queue: defaultTestQueueCaller{}, consumer: consInstTest},
			msgsTest,
			nil,
			consInstTest,
		},
		{
			&DefaultIterator{queue: defaultTestQueueCaller{}},
			msgsTest,
			nil,
			consInstTest,
		},
		{
			&DefaultIterator{queue: consumeMsgErrorQueueCaller{}, consumer: consInstTest},
			nil,
			errors.New("Error while consuming"),
			nil,
		},
	}

	for _, test := range tests {
		actMsgs, actErr := test.iterator.consume()
		if !reflect.DeepEqual(actMsgs, test.expMsgs) || !reflect.DeepEqual(test.iterator.consumer, test.expCons) || !reflect.DeepEqual(test.expErr, actErr) {
			t.Errorf("Expected: msgs: %v, error: %v, consumer: %v\nActual: msgs: %v, error: %v consumer: %v.",
				test.expMsgs, test.expErr, test.expCons, actMsgs, actErr, test.iterator.consumer)
		}
	}
}

var consInstTest = &consumer{"/queue/consumergroup/instance-d", "/instance-id"}
var msgsTest = []Message{Message{nil, "body"}, Message{map[string]string{"Message-Id": "0000-1111-0000-abcd"}, "[]"}}

//test queueCaller implementations

//default happy-case behaviour
type defaultTestQueueCaller struct{}

func (qc defaultTestQueueCaller) createConsumerInstance() (consumer, error) {
	return *consInstTest, nil
}

func (qc defaultTestQueueCaller) destroyConsumerInstance(cInst consumer) error {
	if len(cInst.BaseURI) == 0 && len(cInst.InstanceID) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

func (qc defaultTestQueueCaller) consumeMessages(cInst consumer) ([]Message, error) {
	if len(cInst.BaseURI) == 0 && len(cInst.InstanceID) == 0 {
		return nil, errors.New("consumer instance is nil")
	}
	return msgsTest, nil
}

func (qc defaultTestQueueCaller) commitOffsets(cInst consumer) error {
	if len(cInst.BaseURI) == 0 && len(cInst.InstanceID) == 0 {
		return errors.New("consumer instance is nil")
	}
	return nil
}

//return error on consume and destroy
type consumeMsgErrorQueueCaller struct {
	qc defaultTestQueueCaller
}

func (qc consumeMsgErrorQueueCaller) createConsumerInstance() (consumer, error) {
	return qc.qc.createConsumerInstance()
}

func (qc consumeMsgErrorQueueCaller) destroyConsumerInstance(cInst consumer) error {
	return errors.New("Error while destroying")
}

func (qc consumeMsgErrorQueueCaller) consumeMessages(cInst consumer) ([]Message, error) {
	return nil, errors.New("Error while consuming")
}

func (qc consumeMsgErrorQueueCaller) commitOffsets(cInst consumer) error {
	return errors.New("Error while commiting offsets")
}
