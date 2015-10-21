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

type QueueConfig struct {
	Addr   string `json:"address"`
	Group  string `json:"group"`
	Topic  string `json:"topic"`
	Queue  string `json:"queue"`
	caller proxyCaller
}

type proxyCaller interface {
	DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
}

type consumer struct {
	BaseURI    string `json:"base_uri"`
	InstanceId string `json:",instance_id"`
}

const createConsumerReq = `{"auto.offset.reset": "smallest", "auto.commit.enable": "true"}`

func (q QueueConfig) createConsumerInstance() (c consumer, err error) {
	data, err := q.caller.DoReq("POST", q.Addr+"/consumers/"+q.Group, strings.NewReader(createConsumerReq), map[string]string{"Content-Type": "application/json"}, http.StatusOK)
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

func (q QueueConfig) destroyConsumerInstance(c consumer) (err error) {
	url, _ := q.buildConsumerURL(c)
	_, err = q.caller.DoReq("DELETE", url.String(), nil, nil, http.StatusNoContent)
	return
}

func (q QueueConfig) consumeMessages(c consumer) ([]Message, error) {
	uri, _ := q.buildConsumerURL(c)
	uri.Path = strings.TrimRight(uri.Path, "/") + "/topics/" + q.Topic
	data, err := q.caller.DoReq("GET", uri.String(), nil, map[string]string{"Accept": "application/json"}, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return parseResponse(data)
}

func (q QueueConfig) buildConsumerURL(c consumer) (baseUrl *url.URL, err error) {
	uri, err := url.Parse(c.BaseURI)
	if err != nil {
		log.Printf("ERROR - parsing base URI: %s", err.Error())
		return
	}
	addrUrl, err := url.Parse(q.Addr)
	if err != nil {
		log.Printf("ERROR - parsing Addr: %s", err.Error())
	}
	uri.Host = addrUrl.Host
	return uri, err
}

type defaultProxyCaller struct {
	host string
}

func (p defaultProxyCaller) DoReq(method, url string, body io.Reader, headers map[string]string, expectedStatus int) (data []byte, err error) {
	client := http.Client{}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Printf("ERROR - creating request: %s", err.Error())
		return
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}
	if len(p.host) > 0 {
		req.Host = p.host
	}

	resp, err := client.Do(req)
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
