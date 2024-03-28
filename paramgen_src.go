// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package kafkabroker

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (SourceConfig) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"addr": {
			Default:     "0.0.0.0:9092",
			Description: "address for broker to listen on.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
	}
}
