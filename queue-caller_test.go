package consumer

import (
	"io"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildConsumerURL(t *testing.T) {
	var tests = []struct {
		c        consumerInstanceURI
		q        kafkaRESTClient
		expected string
	}{
		{
			c: testConsumer,
			q: kafkaRESTClient{
				addrs:   []string{"https://localhost:8080/__kafka-rest-proxy"},
				addrInd: 0,
			},
			expected: "https://localhost:8080/__kafka-rest-proxy/consumers/group1/instances/rest-consumer-1-45864",
		},
		{
			c: testConsumer,
			q: kafkaRESTClient{
				addrs:   []string{"http://kafka-proxy.prod.ft.com"},
				addrInd: 0,
			},
			expected: "http://kafka-proxy.prod.ft.com/consumers/group1/instances/rest-consumer-1-45864",
		},
		{
			c: testConsumer,
			q: kafkaRESTClient{
				addrs:   []string{"http://kafka-proxy-1.prod.ft.com", "http://kafka-proxy-2.prod.ft.com"},
				addrInd: 0,
			},
			expected: "http://kafka-proxy-1.prod.ft.com/consumers/group1/instances/rest-consumer-1-45864",
		},

		{
			c: testConsumer,
			q: kafkaRESTClient{
				addrs:   []string{"http://kafka-proxy-1.prod.ft.com", "http://kafka-proxy-2.prod.ft.com"},
				addrInd: 1,
			},
			expected: "http://kafka-proxy-2.prod.ft.com/consumers/group1/instances/rest-consumer-1-45864",
		},
		{
			c: consumerInstanceURI{
				BaseURI: "http://kafka-rest%3A8080/consumers/group1/instances/rest-consumer-1-45864",
			},
			q: kafkaRESTClient{
				addrs:   []string{"https://kafka-rest-proxy"},
				addrInd: 0,
			},
			expected: "https://kafka-rest-proxy/consumers/group1/instances/rest-consumer-1-45864",
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
	queueCaller := &kafkaRESTClient{
		addrs:  []string{"http://kafka-proxy-1.prod.ft.com", "http://kafka-proxy-2.prod.ft.com", "http://kafka-proxy-3.prod.ft.com"},
		caller: testHTTPCaller{},
	}

	_, err := queueCaller.createConsumerInstance()
	if err != nil {
		t.Errorf("Error [%v]", err)
	}
	if queueCaller.addrInd != 1 {
		t.Errorf("Failure: active address index is not correct. Expected: %d. Actual: %d.", 1, queueCaller.addrInd)
	}

	_, err = queueCaller.createConsumerInstance()
	if err != nil {
		t.Errorf("Error [%v]", err)
	}
	if queueCaller.addrInd != 2 {
		t.Errorf("Failure: active address index is not correct. Expected: %d. Actual: %d.", 2, queueCaller.addrInd)
	}

	_, err = queueCaller.createConsumerInstance()
	if err != nil {
		t.Errorf("Error [%v]", err)
	}
	if queueCaller.addrInd != 0 {
		t.Errorf("Failure: active address index is not correct. Expected: %d. Actual: %d.", 0, queueCaller.addrInd)
	}

}

var testConsumer = consumerInstanceURI{
	BaseURI: "http://kafka/consumers/group1/instances/rest-consumer-1-45864",
}

type testHTTPCaller struct {
}

func (t testHTTPCaller) DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error) {
	_, err := url.Parse(addr)
	return []byte("{}"), err
}

func TestNoQueueAddressesFails(t *testing.T) {
	q := kafkaRESTClient{}
	err := q.checkConnectivity()

	assert.EqualError(t, err, ErrNoQueueAddresses.Error())
}
