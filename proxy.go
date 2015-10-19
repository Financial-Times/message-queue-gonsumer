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

type proxy struct {
	addr   string
	topic  string
	group  string
	queue  string
	caller proxyCaller
}

type consumer struct {
	BaseURI    string `json:"base_uri"`
	InstanceId string `json:",instance_id"`
}

type message struct {
	//to implement
}

type proxyCaller interface {
	DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
}

type defaultProxyCaller struct{}

const createConsumerReq = `{"auto.offset.reset": "smallest", "auto.commit.enable": "true"}`

func (p proxy) createConsumerInstance() (c consumer, err error) {

	data, err := p.caller.DoReq("POST", p.addr+"/consumers/"+p.group, strings.NewReader(createConsumerReq), map[string]string{"Content-Type": "application/json"}, http.StatusOK)

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
	req.Host = "kafka"

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

func (p proxy) destroyConsumerInstance(c consumer) (err error) {
	url, _ := p.buildDeleteConsumerURL(c)
	_, err = p.caller.DoReq("DELETE", url, nil, nil, http.StatusNoContent)
	//to implement
	return
}

func (p proxy) consumeMessages(c consumer) ([]Message, error) {
	url, _ := p.buildConsumeMsgsURL(c)
	_, _ = p.caller.DoReq("GET", url, nil, map[string]string{"Accept": "application/json"}, http.StatusOK)
	//to implement
	return make([]Message, 0, 0), nil
}

func (p proxy) buildDeleteConsumerURL(c consumer) (baseUrl string, err error) {
	uri, err := url.Parse(c.BaseURI)
	if err != nil {
		log.Printf("ERROR - parsing base URI: %s", err.Error())
		return
	}
	uri.Host = p.addr
	return uri.String(), nil
}

func (p proxy) buildConsumeMsgsURL(c consumer) (baseUrl string, err error) {
	//to implement
	return "", nil
}
