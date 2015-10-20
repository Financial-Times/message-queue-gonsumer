package consumer

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"regexp"
	"strings"
)

type message struct {
	Value     string //base64 encoded
	Partition string
	Offset    string
}

func parseResponse(data []byte) []Message {
	var resp []message
	err := json.Unmarshal(data, &resp)
	if err != nil {
		log.Printf("ERROR - parsing json message %q failed with error %v", data, err.Error())
		return nil
	}
	msgs := make([]Message, len(resp))
	for _, m := range resp {
		log.Printf("DEBUG - parsing msg of partition %s and offset %s", m.Partition, m.Offset)
		msgs = append(msgs, parseMessage(m.Value))
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

var re = regexp.MustCompile("[\\w-]*:[\\w\\-:/. ]*")

var kre = regexp.MustCompile("[\\w-]*:")
var vre = regexp.MustCompile(":[\\w-:/. ]*")

func parseHeaders(msg string) map[string]string {
	//naive
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
func parseBody(msg string) string {
	//naive
	f := strings.Index(msg, "{")
	l := strings.LastIndex(msg, "}")
	return msg[f : l+1]
}
