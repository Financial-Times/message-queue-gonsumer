package consumer

import (
	"testing"
)

func TestBuildConsumerURL(t *testing.T) {
	var tests = []struct {
		c        consumer
		q        QueueConfig
		expected string
	}{
		{
			testConsumer,
			QueueConfig{
				Addr: "http://localhost:8080",
			},
			"http://localhost:8080/consumers/zoli/instances/rest-consumer-1-45864",
		},
		{
			testConsumer,
			QueueConfig{
				Addr: "http://kafka-proxy.prod.ft.com",
			},
			"http://kafka-proxy.prod.ft.com/consumers/zoli/instances/rest-consumer-1-45864",
		},
	}

	for _, test := range tests {
		actual, err := test.q.buildConsumerURL(test.c)
		if err != nil {
			t.Errorf("Error: %s", err.Error())
		}
		if actual.String() != test.expected {
			t.Errorf("Expected: %s\nActual: %s", test.expected, actual)
		}

	}
}

var testConsumer = consumer{
	"http://kafka/consumers/zoli/instances/rest-consumer-1-45864",
	"rest-consumer-1-45864",
}
