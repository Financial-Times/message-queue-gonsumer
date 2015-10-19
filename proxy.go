package consumer

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
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

type proxyCaller interface {
	DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
}

type defaultProxyCaller struct {
}

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
	return
}

func (p proxy) consumeMessages(c consumer) ([]Message, error) {
	url, _ := p.buildConsumeMsgsURL(c)
	data, err := p.caller.DoReq("GET", url, nil, map[string]string{"Accept": "application/json"}, http.StatusOK)
	if err != nil {
		return nil, err
	}

	return parseResponse(data), nil
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
	uri, err := url.Parse(c.BaseURI)
	if err != nil {
		log.Printf("ERROR - parsing base URI: %s", err.Error())
		return
	}
	uri.Host = p.addr
	uri.Path = strings.TrimRight(uri.Path, "/") + "/topics/" + p.topic
	return uri.String(), nil
}

func parseResponse(data []byte) []Message {
	var resp *response
	err := json.Unmarshal(data, resp)
	if err != nil {
		log.Printf("ERROR - parsing json message: %v", err.Error())
		return nil
	}

	msgs := make([]Message, len(resp.msg))
	for _, m := range resp.msg {
		log.Printf("DEBUG - parsing msg of partition %s and offset %s", m.partition, m.offset)
		msgs = append(msgs, parseMessage(m.value))
	}

	return msgs
}

func parseMessage(raw string) (m Message) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		log.Printf("ERROR - failure in decoding base64 value: %s", err.Error())
		return
	}
	m.Headers = parseHeaders(string(decoded[:]))
	m.Body = parseBody(string(decoded[:]))
	return
}

func parseBody(msg string) string {
	//naive
	f := strings.Index(msg, "{")
	l := strings.LastIndex(msg, "}")
	return msg[f : l+1]
}

var re = regexp.MustCompile("[\\w-]*:[\\w\\-:/. ]*")

var kre = regexp.MustCompile("[\\w-]*:")
var vre = regexp.MustCompile(":[\\w-:/. ]*")

func parseHeaders(msg string) map[string]string {
	i := strings.Index(msg, "{")
	headerLines := re.FindAllString(msg[:i], -1)

	headers := make(map[string]string)
	for _, line := range headerLines {
		key, value := parseHeader(line)
		headers[key] = value
	}
	return headers
}

func parseHeader(header string) (string, string) {
	key := kre.FindString(header)
	value := vre.FindString(header)
	return key[:len(key)-1], strings.TrimSpace(value[1:])
}

type response struct {
	msg []message
}

type message struct {
	value     string //base64 encoded
	partition string
	offset    string
}
