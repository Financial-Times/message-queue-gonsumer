package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrNoQueueAddresses = errors.New("no kafka-rest-proxy addresses configured")

const msgContentType = "application/vnd.kafka.v2+json"

type defaultQueueCaller struct {
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

type httpCaller interface {
	DoReq(method, addr string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error)
}

func (q *defaultQueueCaller) createConsumerInstance() (c consumer, err error) {
	q.addrInd = (q.addrInd + 1) % len(q.addrs)
	addr := q.addrs[q.addrInd]

	reqBody := strings.NewReader(`{"auto.offset.reset": "` + q.offset + `", "auto.commit.enable": "` + strconv.FormatBool(q.autoCommitEnable) + `"}`)
	data, err := q.caller.DoReq("POST", addr+"/consumers/"+q.group, reqBody, map[string]string{"Content-Type": msgContentType}, http.StatusOK)
	if err != nil {
		return consumer{}, err
	}
	err = json.Unmarshal(data, &c)
	if err != nil {
		return consumer{}, fmt.Errorf("error unmarshalling json content: %w", err)
	}

	return
}

func (q *defaultQueueCaller) destroyConsumerInstance(c consumer) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	_, err = q.caller.DoReq("DELETE", url.String(), nil, map[string]string{"Accept": msgContentType}, http.StatusNoContent)
	return err
}

func (q *defaultQueueCaller) subscribeConsumerInstance(c consumer) (err error) {
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

func (q *defaultQueueCaller) destroyConsumerInstanceSubscription(c consumer) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	url.Path = strings.TrimRight(url.Path, "/") + "/subscription"
	_, err = q.caller.DoReq("DELETE", url.String(), nil, map[string]string{"Accept": msgContentType}, http.StatusNoContent)
	return err
}

func (q *defaultQueueCaller) consumeMessages(c consumer) ([]byte, error) {
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

func (q *defaultQueueCaller) commitOffsets(c consumer) (err error) {
	url, err := q.buildConsumerURL(c)
	if err != nil {
		return fmt.Errorf("error building consumer URL: %w", err)
	}

	url.Path = strings.TrimRight(url.Path, "/") + "/offsets"
	_, err = q.caller.DoReq("POST", url.String(), nil, map[string]string{"Content-Type": msgContentType}, http.StatusOK)

	return err
}

func (q *defaultQueueCaller) buildConsumerURL(c consumer) (uri *url.URL, err error) {
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

type httpClient struct {
	hostHeader       string
	authorizationKey string
	client           *http.Client
}

func (c httpClient) DoReq(method, url string, body io.Reader, headers map[string]string, expectedStatus int) (data []byte, err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}
	if len(c.hostHeader) > 0 {
		req.Host = c.hostHeader
	}

	if len(c.authorizationKey) > 0 {
		req.Header.Add("Authorization", c.authorizationKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			// This might be a problem with the server instance, which may have been taken out
			// of the DNS pool, but because we might still have a tcp connection open, we'll
			// never re-do the DNS lookup and get a connection to a working server.  So when we
			// get 5xx, close idle connections to force the next requests to re-connect.
			if t, ok := c.client.Transport.(*http.Transport); ok {
				t.CloseIdleConnections()
			}
		}
	}()

	if resp.StatusCode != expectedStatus {
		return nil, fmt.Errorf("unexpected response status %d. Expected: %d", resp.StatusCode, expectedStatus)
	}

	return ioutil.ReadAll(resp.Body)
}

func (q *defaultQueueCaller) checkConnectivity() error {
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

func (q *defaultQueueCaller) checkMessageQueueProxyReachable(address string) error {
	_, err := q.caller.DoReq("GET", address+"/topics", nil, map[string]string{"Accept": msgContentType}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("could not connect to proxy: %w", err)
	}

	return nil
}
