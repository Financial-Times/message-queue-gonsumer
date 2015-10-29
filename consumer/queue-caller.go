package consumer

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type queueCaller interface {
	createConsumerInstance() (consumer, error)
	consumeMessages(c consumer) ([]Message, error)
	destroyConsumerInstance(c consumer) error
}

type defaultQueueCaller struct {
	addr   string
	group  string
	topic  string
	caller httpCaller
}

type httpCaller interface {
	doReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
	initializeClient()
}

type consumer struct {
	BaseURI    string `json:"base_uri"`
	InstanceID string `json:",instance_id"`
}

const createConsumerReq = `{"auto.offset.reset": "smallest", "auto.commit.enable": "true"}`

func (q defaultQueueCaller) createConsumerInstance() (c consumer, err error) {
	q.caller.initializeClient()
	data, err := q.caller.doReq("POST", q.addr+"/consumers/"+q.group, strings.NewReader(createConsumerReq), map[string]string{"Content-Type": "application/json"}, http.StatusOK)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &c)
	if err != nil {
		log.Printf("ERROR - unmarshalling json content: %s", err.Error())
		return
	}
	return
}

func (q defaultQueueCaller) destroyConsumerInstance(c consumer) (err error) {
	url, _ := q.buildConsumerURL(c)
	_, err = q.caller.doReq("DELETE", url.String(), nil, nil, http.StatusNoContent)
	return
}

func (q defaultQueueCaller) consumeMessages(c consumer) ([]Message, error) {
	uri, _ := q.buildConsumerURL(c)
	uri.Path = strings.TrimRight(uri.Path, "/") + "/topics/" + q.topic
	data, err := q.caller.doReq("GET", uri.String(), nil, map[string]string{"Accept": "application/json"}, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return parseResponse(data)
}

func (q defaultQueueCaller) buildConsumerURL(c consumer) (uri *url.URL, err error) {
	uri, err = url.Parse(c.BaseURI)
	if err != nil {
		log.Printf("ERROR - parsing base URI: %s", err.Error())
		return
	}
	addrURL, err := url.Parse(q.addr)
	if err != nil {
		log.Printf("ERROR - parsing Addr: %s", err.Error())
	}
	uri.Host = addrURL.Host
	return uri, err
}

type defaultHTTPCaller struct {
	host   string
	client http.Client
}

func (caller defaultHTTPCaller) doReq(method, url string, body io.Reader, headers map[string]string, expectedStatus int) (data []byte, err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Printf("ERROR - creating request: %s", err.Error())
		return
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}
	if len(caller.host) > 0 {
		req.Host = caller.host
	}

	resp, err := caller.client.Do(req)
	if err != nil {
		log.Printf("ERROR - executing request: %s", err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != expectedStatus {
		err = fmt.Errorf("Unexpected response status %d. Expected: %d.", resp.StatusCode, expectedStatus)
		log.Printf("ERROR - %s", err.Error())
		return
	}

	return ioutil.ReadAll(resp.Body)
}

func (caller defaultHTTPCaller) initializeClient() {
	caller.client = http.Client{}
}
