# Conduit Connector acting as a Kafka Broker

[Conduit](https://conduit.io) connector that exposes a Kafka Broker interface.

> [!WARNING]  
> This connector is experimental, do not use in production.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests.

## Source

A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.

### Configuration

| name   | description                              | required | default value  |
|--------|------------------------------------------|----------|----------------|
| `addr` | The address for the broker to listen on. | false    | `0.0.0.0:9092` |

## Destination

The destination is currently not implemented.