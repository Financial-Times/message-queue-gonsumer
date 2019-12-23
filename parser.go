package consumer

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	log "github.com/Financial-Times/go-logger/v2"
)

//raw message
type message struct {
	Value     string `json:"value"` //base64 encoded
	Partition int    `json:"partition"`
	Offset    int    `json:"offset"`
}

func parseResponse(data []byte, logger *log.UPPLogger) ([]Message, error) {
	var resp []message
	err := json.Unmarshal(data, &resp)
	if err != nil {
		return nil, fmt.Errorf("error parsing json message %q: %w", data, err)
	}
	var msgs []Message
	for _, m := range resp {
		msg, err := parseMessage(m.Value, logger)
		if err != nil {
			logger.WithError(err).Error("Error parsing message")
			continue
		}

		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// FT async msg format:
//
// message-version CRLF
// *(message-header CRLF)
// CRLF
// message-body
func parseMessage(raw string, logger *log.UPPLogger) (m Message, err error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return Message{}, fmt.Errorf("error decoding base64 value: %w", err)
	}
	doubleNewLineStartIndex, err := getHeaderSectionEndingIndex(string(decoded[:]))
	if err != nil {
		doubleNewLineStartIndex = len(decoded)
		logger.WithError(err).Warn("message with no message body")
	}

	m.Headers = parseHeaders(string(decoded[:doubleNewLineStartIndex]))
	m.Body = strings.TrimSpace(string(decoded[doubleNewLineStartIndex:]))
	return m, nil
}

func getHeaderSectionEndingIndex(msg string) (int, error) {
	//FT msg format uses CRLF for line endings
	i := strings.Index(msg, "\r\n\r\n")
	if i != -1 {
		return i, nil
	}
	//fallback to UNIX line endings
	i = strings.Index(msg, "\n\n")
	if i != -1 {
		return i, nil
	}

	return 0, errors.New("header section ending not found")
}

var re = regexp.MustCompile(`[\w-]*:[\w\-:/.+;= ]*`)
var kre = regexp.MustCompile(`[\w-]*:`)
var vre = regexp.MustCompile(`:[\w-:/.+;= ]*`)

func parseHeaders(msg string) map[string]string {
	headerLines := re.FindAllString(msg, -1)
	if headerLines == nil {
		return nil
	}

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
