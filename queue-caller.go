package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrNoQueueAddresses = errors.New("no kafka-rest-proxy addresses configured")

const msgContentType = "application/vnd.kafka.v2+json"

type httpCaller interface {
	DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
}

type kafkaRESTClient struct {
	//pool of queue addresses
	//the active address is changed in a round-robin fashion before each new consumer instance creation
	addrs []string
	//used queue addr
	//this gets 'incremented modulo' at each createConsumerInstance() call
	addrInd          int
	group            string
	topic            string
	offset           string
	caller           httpCaller
	autoCommitEnable bool
}

func (q *kafkaRESTClient) createConsumerInstance() (c consumerInstanceURI, err error) {
	q.addrInd = (q.addrInd + 1) % len(q.addrs)
	addr := q.addrs[q.addrInd]

	reqBody := strings.NewReader(`{"auto.offset.reset": "` + q.offset + `", "auto.commit.enable": "` + strconv.FormatBool(q.autoCommitEnable) + `"}`)
	data, err := q.caller.DoReq("POST", addr+"/consumers/"+q.group, reqBody, map[string]string{"Content-Type": msgContentType}, http.StatusOK)
	if err != nil {
		return consumerInstanceURI{}, err
	}
	err = json.Unmarshal(data, &c)
	if err != nil {
		return consumerInstanceURI{}, fmt.Errorf("error unmarshalling json content: %w", err)
	}

	return
}

func (q *kafkaRESTClient) destroyConsumerInstance(c consumerInstanceURI) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	_, err = q.caller.DoReq("DELETE", url.String(), nil, map[string]string{"Accept": msgContentType}, http.StatusNoContent)
	return err
}

func (q *kafkaRESTClient) subscribeConsumerInstance(c consumerInstanceURI) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	url.Path = strings.TrimRight(url.Path, "/") + "/subscription"
	reqBody := strings.NewReader(`{"topics": ["` + q.topic + `"]}`)
	_, err = q.caller.DoReq("POST", url.String(), reqBody, map[string]string{"Content-Type": msgContentType}, http.StatusNoContent)
	if err != nil {
		return err
	}

	return
}

func (q *kafkaRESTClient) destroyConsumerInstanceSubscription(c consumerInstanceURI) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	url.Path = strings.TrimRight(url.Path, "/") + "/subscription"
	_, err = q.caller.DoReq("DELETE", url.String(), nil, map[string]string{"Accept": msgContentType}, http.StatusNoContent)
	return err
}

func (q *kafkaRESTClient) consumeMessages(c consumerInstanceURI) ([]byte, error) {
	uri, err := q.buildConsumerURL(c)
	if err != nil {
		return nil, fmt.Errorf("error building consumer URL: %w", err)
	}

	uri.Path = strings.TrimRight(uri.Path, "/") + "/records"
	data, err := q.caller.DoReq("GET", uri.String(), nil, map[string]string{"Accept": msgContentType}, http.StatusOK)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (q *kafkaRESTClient) commitOffsets(c consumerInstanceURI) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	url.Path = strings.TrimRight(url.Path, "/") + "/offsets"
	_, err = q.caller.DoReq("POST", url.String(), nil, map[string]string{"Content-Type": msgContentType}, http.StatusOK)

	return err
}

func (q *kafkaRESTClient) buildConsumerURL(c consumerInstanceURI) (uri *url.URL, err error) {
	// In some cases the REST proxy returns encoded symbols in the URL
	baseURI, err := url.QueryUnescape(c.BaseURI)
	if err != nil {
		return nil, fmt.Errorf("unsupported base URI value: %w", err)
	}

	uri, err = url.Parse(baseURI)
	if err != nil {
		return nil, fmt.Errorf("error parsing base URI: %w", err)
	}

	addr := q.addrs[q.addrInd]
	addrURL, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("error parsing queue address: %w", err)
	}
	addrURL.Path = addrURL.Path + uri.Path
	return addrURL, nil
}

func (q *kafkaRESTClient) checkConnectivity() error {
	if len(q.addrs) == 0 {
		return ErrNoQueueAddresses
	}

	errMsg := ""
	for _, address := range q.addrs {
		if err := q.checkMessageQueueProxyReachable(address); err != nil {
			errMsg = errMsg + err.Error() + "; "
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (q *kafkaRESTClient) checkMessageQueueProxyReachable(address string) error {
	_, err := q.caller.DoReq("GET", address+"/topics", nil, map[string]string{"Accept": msgContentType}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("could not connect to proxy: %w", err)
	}

	return nil
}
