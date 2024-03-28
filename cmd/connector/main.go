package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	kafkabroker "github.com/lovromazgon/conduit-connector-kafka-broker"
)

func main() {
	sdk.Serve(kafkabroker.Connector)
}
