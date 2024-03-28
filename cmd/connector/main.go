package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	kafka-broker "github.com/lovromazgon/conduit-connector-kafka-broker"
)

func main() {
	sdk.Serve(kafka-broker.Connector)
}
