package consumer

import (
	"errors"
	"net/http"
	"time"

	log "github.com/Financial-Times/go-logger/v2"
)

// NewAgeingClient returns a new instance of AgeingClient. It guarantees that all required properties are set
func NewAgeingClient(client *http.Client, maxAge time.Duration, logger *log.UPPLogger) (*AgeingClient, error) {
	if client == nil {
		return &AgeingClient{}, errors.New("non-nil HTTP client required")
	}
	if logger == nil {
		return &AgeingClient{}, errors.New("non-nil UPPLogger required")
	}

	return &AgeingClient{
		HTTPClient: client,
		MaxAge:     maxAge,
		Logger:     logger,
	}, nil
}

//AgeingClient defines an ageing http client for consuming messages
type AgeingClient struct {
	HTTPClient *http.Client
	MaxAge     time.Duration
	Logger     *log.UPPLogger
}

//StartAgeingProcess periodically close idle connections according to the MaxAge of an AgeingClient
func (c AgeingClient) StartAgeingProcess() {
	c.Logger.Infof("Starting aging [%d]", c.MaxAge)
	ticker := time.NewTicker(c.MaxAge)
	go func() {
		for range ticker.C {
			c.Logger.Info("Closing idle connections")
			c.HTTPClient.Transport.(*http.Transport).CloseIdleConnections()
		}
	}()
}
