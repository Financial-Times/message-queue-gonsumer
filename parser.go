package consumer

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"regexp"
	"strings"
)

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
