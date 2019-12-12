package consumer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
)

const mockedTopics = `["methode-articles","up-placeholders"]`

var consumerConfigMock = QueueConfig{
	Group:            "mcpm-group",
	Topic:            "methode-articles",
	Queue:            "host",
	AuthorizationKey: "my-first-auth-key",
}

func setupMockKafka(t *testing.T, status int, response string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if status != 200 {
			w.WriteHeader(status)
		} else {
			w.Write([]byte(response))
		}
		assert.Equal(t, "/topics", req.URL.Path)
		assert.Equal(t, "my-first-auth-key", req.Header.Get("Authorization"))
	}))
}

func TestHappyConnectivityCheck(t *testing.T) {
	proxy1 := setupMockKafka(t, 200, mockedTopics)
	defer proxy1.Close()
	proxy2 := setupMockKafka(t, 200, mockedTopics)
	defer proxy2.Close()
	proxy3 := setupMockKafka(t, 200, mockedTopics)
	defer proxy3.Close()

	consumerConfigMock.Addrs = []string{proxy1.URL, proxy2.URL, proxy3.URL}
	log := logger.NewUPPLogger("Test", "FATAL")
	c := NewConsumer(consumerConfigMock, func(m Message) {}, &http.Client{}, log)
	msg, err := c.ConnectivityCheck()

	assert.NoError(t, err, "It should not return an error")
	assert.Equal(t, "Connectivity to consumer proxies is OK.", msg, `The check message should be "Connectivity to consumer proxies is OK."`)
}

func TestConnectivityCheckUnhappyKakfka(t *testing.T) {
	proxy1 := setupMockKafka(t, 200, mockedTopics)
	defer proxy1.Close()
	proxy2 := setupMockKafka(t, 500, "")
	defer proxy2.Close()
	proxy3 := setupMockKafka(t, 200, mockedTopics)
	defer proxy3.Close()

	consumerConfigMock.Addrs = []string{proxy1.URL, proxy2.URL, proxy3.URL}
	log := logger.NewUPPLogger("Test", "FATAL")
	c := NewConsumer(consumerConfigMock, func(m Message) {}, &http.Client{}, log)
	msg, err := c.ConnectivityCheck()

	assert.EqualError(t, err, "could not connect to proxy: unexpected response status 500. Expected: 200; ", "It should return an error")
	assert.Equal(t, "Error connecting to consumer proxies", msg, `The check message should be "Error connecting to consumer proxies"`)
}

func TestConnectivityCheckNoKafka(t *testing.T) {
	proxy1 := setupMockKafka(t, 200, mockedTopics)
	defer proxy1.Close()
	proxy2 := setupMockKafka(t, 200, mockedTopics)
	defer proxy2.Close()

	consumerConfigMock.Addrs = []string{proxy1.URL, proxy2.URL, "http://do.not.exist.com/"}
	log := logger.NewUPPLogger("Test", "FATAL")
	c := NewConsumer(consumerConfigMock, func(m Message) {}, &http.Client{}, log)
	msg, err := c.ConnectivityCheck()

	assert.Error(t, err, "It should return an error")
	assert.Equal(t, "Error connecting to consumer proxies", msg, `The check message should be "Error connecting to consumer proxies"`)
}
