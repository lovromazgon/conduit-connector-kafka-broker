package kafkabroker

// Config contains shared config parameters, common to the source and
// destination.
type Config struct {
	// Address for broker to listen on.
	Addr string `json:"addr" default:"0.0.0.0:9092"`
}
