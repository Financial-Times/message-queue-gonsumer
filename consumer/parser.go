package consumer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

//raw message
type message struct {
	Value     string `json:"value"` //base64 encoded
	Partition int    `json:"partition"`
	Offset    int    `json:"offset"`
}

func parseResponse(data []byte) ([]Message, error) {
	var resp []message
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, err
	}
	var msgs []Message
	for _, m := range resp {
		if msg, err := parseMessage(m.Value); err == nil {
			msgs = append(msgs, msg)
		} else {
			warnLogger.Printf("Error while parsing message: partition [%d], offset [%d]. Cause: [%v]. Skipping.", m.Partition, m.Offset, err)
		}
	}
	return msgs, nil
}

func parseMessage(raw string) (m Message, err error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		wrappedErr := fmt.Errorf("Error in decoding base64 value: [%v]", err)
		err = wrappedErr
		return
	}
	if m.Headers, err = parseHeaders(string(decoded[:])); err != nil {
		return
	}
	if m.Body, err = parseBody(string(decoded[:])); err != nil {
		return
	}
	return
}

var re = regexp.MustCompile("[\\w-]*:[\\w\\-:/. ]*")

var kre = regexp.MustCompile("[\\w-]*:")
var vre = regexp.MustCompile(":[\\w-:/. ]*")

func parseHeaders(msg string) (map[string]string, error) {
	//naive
	i := strings.Index(msg, "{")
	if i == -1 {
		return nil, fmt.Errorf("Cannot parse headers: cannot find '{' character. Message: [%s]", msg)
	}
	headerLines := re.FindAllString(msg[:i], -1)

	headers := make(map[string]string)
	for _, line := range headerLines {
		key, value := parseHeader(line)
		headers[key] = value
	}
	return headers, nil
}

func parseHeader(header string) (string, string) {
	key := kre.FindString(header)
	value := vre.FindString(header)
	return key[:len(key)-1], strings.TrimSpace(value[1:])
}
func parseBody(msg string) (string, error) {
	//naive
	f := strings.Index(msg, "{")
	l := strings.LastIndex(msg, "}")
	if f == -1 || l == -1 {
		return "", fmt.Errorf("Cannot parse body: cannot find '{' or '}' characters. Message: [%s]", msg)
	}
	return msg[f : l+1], nil
}
