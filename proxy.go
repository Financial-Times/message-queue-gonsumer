package consumer

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type QueueConfig struct {
	Addr   string
	Group  string
	Topic  string
	Queue  string
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
	url, _ := q.buildDeleteConsumerURL(c)
	_, err = q.caller.DoReq("DELETE", url, nil, nil, http.StatusNoContent)
	return
}

func (q QueueConfig) consumeMessages(c consumer) ([]Message, error) {
	url, _ := q.buildConsumeMsgsURL(c)
	data, err := q.caller.DoReq("GET", url, nil, map[string]string{"Accept": "application/json"}, http.StatusOK)
	if err != nil {
		return nil, err
	}

	return parseResponse(data), nil
}

func (q QueueConfig) buildDeleteConsumerURL(c consumer) (baseUrl string, err error) {
	uri, err := url.Parse(c.BaseURI)
	if err != nil {
		log.Printf("ERROR - parsing base URI: %s", err.Error())
		return
	}
	uri.Host = q.Addr
	return uri.String(), nil
}

func (q QueueConfig) buildConsumeMsgsURL(c consumer) (baseUrl string, err error) {
	uri, err := url.Parse(c.BaseURI)
	if err != nil {
		log.Printf("ERROR - parsing base URI: %s", err.Error())
		return
	}
	uri.Host = q.Addr
	uri.Path = strings.TrimRight(uri.Path, "/") + "/topics/" + q.Topic
	return uri.String(), nil
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
		log.Printf("ERROR - unexpected response status: %d", resp.StatusCode)
		return
	}

	return ioutil.ReadAll(resp.Body)
}
