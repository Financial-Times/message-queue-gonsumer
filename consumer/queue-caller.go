package consumer

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
	//pool of queue addresses
	//the active address is changed in a round-robin fashion before each new consumer instance creation
	addrs []string
	//used queue addr
	//this gets 'incremented modulo' at each createConsumerInstance() call
	addrInd int
	group   string
	topic   string
	offset  string
	caller  httpCaller
}

type httpCaller interface {
	DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
}

type consumer struct {
	BaseURI    string `json:"base_uri"`
	InstanceID string `json:",instance_id"`
}

func (q *defaultQueueCaller) createConsumerInstance() (c consumer, err error) {
	q.addrInd = (q.addrInd + 1) % len(q.addrs)
	addr := q.addrs[q.addrInd]

	createConsumerReq := `{"auto.offset.reset": "` + q.offset + `", "auto.commit.enable": "true"}`
	data, err := q.caller.DoReq("POST", addr+"/consumers/"+q.group, strings.NewReader(createConsumerReq), map[string]string{"Content-Type": "application/json"}, http.StatusOK)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &c)
	if err != nil {
		err = fmt.Errorf("Unmarshalling json content: [%v]", err)
		return
	}
	return
}

func (q *defaultQueueCaller) destroyConsumerInstance(c consumer) (err error) {
	url, _ := q.buildConsumerURL(c)
	_, err = q.caller.DoReq("DELETE", url.String(), nil, nil, http.StatusNoContent)
	return
}

func (q *defaultQueueCaller) consumeMessages(c consumer) ([]Message, error) {
	uri, _ := q.buildConsumerURL(c)
	uri.Path = strings.TrimRight(uri.Path, "/") + "/topics/" + q.topic
	data, err := q.caller.DoReq("GET", uri.String(), nil, map[string]string{"Accept": "application/json"}, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return parseResponse(data)
}

func (q *defaultQueueCaller) buildConsumerURL(c consumer) (uri *url.URL, err error) {
	uri, err = url.Parse(c.BaseURI)
	if err != nil {
		err = fmt.Errorf("Parsing base URI: [%v]", err)
		return
	}
	addr := q.addrs[q.addrInd]
	addrURL, err := url.Parse(addr)
	if err != nil {
		err = fmt.Errorf("Parsing addr: [%v]", err)
		return
	}
	uri.Host = addrURL.Host
	uri.Scheme = addrURL.Scheme
	return uri, err
}

type defaultHTTPCaller struct {
	hostHeader       string
	authorizationKey string
	client           http.Client
}

func (caller defaultHTTPCaller) DoReq(method, url string, body io.Reader, headers map[string]string, expectedStatus int) (data []byte, err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		err = fmt.Errorf("Error creating request: [%v]", err)
		return
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}
	if len(caller.hostHeader) > 0 {
		req.Host = caller.hostHeader
	}

	if len(caller.authorizationKey) > 0 {
		req.Header.Add("Authorization", caller.authorizationKey)
	}

	resp, err := caller.client.Do(req)
	if err != nil {
		err = fmt.Errorf("Error executing request: [%v]", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != expectedStatus {
		err = fmt.Errorf("Unexpected response status %d. Expected: %d.", resp.StatusCode, expectedStatus)
		return
	}

	return ioutil.ReadAll(resp.Body)
}
