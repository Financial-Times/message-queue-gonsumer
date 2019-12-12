package consumer

import (
	"net/http"
	"time"

	log "github.com/Financial-Times/go-logger/v2"
)

//AgeingClient defines an ageing http client for consuming messages
type AgeingClient struct {
	Client *http.Client
	MaxAge time.Duration
	Logger *log.UPPLogger
}

//StartAgeingProcess periodically close idle connections according to the MaxAge of an AgeingClient
func (c AgeingClient) StartAgeingProcess() {
	c.Logger.Infof("Starting aging [%d]", c.MaxAge)
	ticker := time.NewTicker(c.MaxAge)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.Logger.Info("Closing idle connections")
				c.Client.Transport.(*http.Transport).CloseIdleConnections()
			}
		}
	}()
}
