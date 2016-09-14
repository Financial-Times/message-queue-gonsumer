package consumer

import (
	"net/http"
	"time"
	"github.com/golang/go/src/pkg/log"
)

type AgeingClient struct {
	Client http.Client

	MaxAge time.Duration
}

func (client AgeingClient) StartAgeingProcess() {
	log.Printf("INFO: Starting aging [%d]", client.MaxAge)
	ticker := time.NewTicker(client.MaxAge)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Print("INFO: Closing idle connections")
				client.Client.Transport.(*http.Transport).CloseIdleConnections()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
