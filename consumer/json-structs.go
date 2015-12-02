package consumer

//QueueConfig represents the configuration of the queue, consumer group and topic the consumer interested about.
type QueueConfig struct {
	//list of queue addresses.
	Addrs []string `json:"address"`
	Group string   `json:"group"`
	Topic string   `json:"topic"`
	//the name of the queue
	//leave it empty for requests to UCS kafka-proxy
	Queue            string `json:"queue"`
	Offset           string `json:"offset"`
	BackoffPeriod    int    `json:backoffPeriod`
	StreamCount      int    `json: streamCount`
	AuthorizationKey string
}

type consumer struct {
	BaseURI    string `json:"base_uri"`
	InstanceID string `json:",instance_id"`
}
