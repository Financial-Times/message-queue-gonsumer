package consumer

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

// Implementation of the httpCaller interface
type httpClient struct {
	hostHeader       string
	authorizationKey string
	client           *http.Client
}

func (c httpClient) DoReq(method, url string, body io.Reader, headers map[string]string, expectedStatus int) ([]byte, error) {
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
