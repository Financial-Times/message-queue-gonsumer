package consumer

import (
	"io"
	"net/url"
	"testing"
)

func TestBuildConsumerURL(t *testing.T) {
	var tests = []struct {
		c        consumer
		q        defaultQueueCaller
		expected string
	}{
		{
			testConsumer,
			defaultQueueCaller{
				addrs:   []string{"https://localhost:8080"},
				addrInd: 0,
			},
			"https://localhost:8080/consumers/zoli/instances/rest-consumer-1-45864",
		},
		{
			testConsumer,
			defaultQueueCaller{
				addrs:   []string{"http://kafka-proxy.prod.ft.com"},
				addrInd: 0,
			},
			"http://kafka-proxy.prod.ft.com/consumers/zoli/instances/rest-consumer-1-45864",
		},
		{
			testConsumer,
			defaultQueueCaller{
				addrs:   []string{"http://kafka-proxy-1.prod.ft.com", "http://kafka-proxy-2.prod.ft.com"},
				addrInd: 0,
			},
			"http://kafka-proxy-1.prod.ft.com/consumers/zoli/instances/rest-consumer-1-45864",
		},

		{
			testConsumer,
			defaultQueueCaller{
				addrs:   []string{"http://kafka-proxy-1.prod.ft.com", "http://kafka-proxy-2.prod.ft.com"},
				addrInd: 1,
			},
			"http://kafka-proxy-2.prod.ft.com/consumers/zoli/instances/rest-consumer-1-45864",
		},
	}

	for _, test := range tests {
		actual, err := test.q.buildConsumerURL(test.c)
		if err != nil {
			t.Errorf("Error: [%s]", err.Error())
		}
		if actual.String() != test.expected {
			t.Errorf("Expected: %s\nActual: %s", test.expected, actual)
		}

	}
}

func TestCreateConsumerInstance_queueAddressesAreChangedInRoundRobinFashion(t *testing.T) {
	queueCaller := &defaultQueueCaller{
		addrs:  []string{"http://kafka-proxy-1.prod.ft.com", "http://kafka-proxy-2.prod.ft.com", "http://kafka-proxy-3.prod.ft.com"},
		caller: testHTTPCaller{},
	}

	_, err := queueCaller.createConsumerInstance()
	if err != nil {
		t.Errorf("Error [%v]", err)
	}
	if queueCaller.addrInd != 1 {
		t.Errorf("Failure: active addres index is not correct. Expected: %d. Actual: %d.", 1, queueCaller.addrInd)
	}

	_, err = queueCaller.createConsumerInstance()
	if err != nil {
		t.Errorf("Error [%v]", err)
	}
	if queueCaller.addrInd != 2 {
		t.Errorf("Failure: active addres index is not correct. Expected: %d. Actual: %d.", 2, queueCaller.addrInd)
	}

	_, err = queueCaller.createConsumerInstance()
	if err != nil {
		t.Errorf("Error [%v]", err)
	}
	if queueCaller.addrInd != 0 {
		t.Errorf("Failure: active addres index is not correct. Expected: %d. Actual: %d.", 0, queueCaller.addrInd)
	}

}

var testConsumer = consumer{
	"http://kafka/consumers/zoli/instances/rest-consumer-1-45864",
	"rest-consumer-1-45864",
}

type testHTTPCaller struct {
}

func (t testHTTPCaller) DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error) {
	_, err := url.Parse(addr)
	return []byte("{}"), err
}
