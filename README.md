# pinax-payment-gateway-kafka-connect

A [Kafka Connect](https://kafka.apache.org/documentation/#connect) **sink** connector that
forwards usage/metering events from a Kafka topic to a [StreamingFast](https://www.streamingfast.io/)
**Payment Gateway** over gRPC.

For each batch of records it:

1. **Parses** every record value as a [`sf.metering.v1.Event`](src/main/proto/sf/metering/v1/metering.proto)
   (protobuf JSON).
2. **Groups** the events by `user_id`.
3. **Reports** them to the Payment Gateway's `UsageService.Report` RPC — one
   [`ReportRequest`](src/main/proto/sf/gateway/payment/v1/gateway.proto) per user — authenticated
   with a Bearer token.

If the gateway responds that a user has been **revoked**, that is logged as a warning.

---

## Requirements

|                   |                                                                               |
|-------------------|-------------------------------------------------------------------------------|
| **Java**          | 17+ (the connector and its `connect-api` dependency are compiled for Java 17) |
| **Kafka Connect** | Runtime built against `connect-api` 3.9                                        |
| **Payment Gateway** | A reachable StreamingFast Payment Gateway `UsageService` endpoint           |
| **Protoc**        | Only for the build — the [`protobuf-maven-plugin`](pom.xml) downloads it automatically |

## Build

```bash
mvn clean package
```

This produces a shaded, dependency-bundled plugin jar:

```
target/pinax-payment-gateway-kafka-connect.jar
```

The build runs `protoc` (via `protobuf-maven-plugin`) to (re)generate the gRPC/protobuf
stubs from [`src/main/proto`](src/main/proto) into `src/main/java/sf/...`.

## Install

Drop the jar into a directory on your Connect worker's `plugin.path`, e.g.:

```
plugin.path=/opt/kafka/connect-plugins
```

```
/opt/kafka/connect-plugins/pinax-payment-gateway-kafka-connect/pinax-payment-gateway-kafka-connect.jar
```

and restart the worker.

## Configuration

| Property       | Required | Default          | Description                                                                                  |
|----------------|----------|------------------|----------------------------------------------------------------------------------------------|
| `endpoint`     | no       | `localhost:8080` | Payment Gateway gRPC endpoint as `host:port` (e.g. `abp.thegraph.market:443`). No scheme.    |
| `token`        | no       | `token`          | Bearer token sent as `Authorization: Bearer <token>` on every request. Must be non-empty.    |
| `usePlaintext` | no       | `false`          | Use a plaintext (non-TLS) gRPC connection. Mutually exclusive with `useInsecure`.            |
| `useInsecure`  | no       | `false`          | Use TLS but **skip server certificate verification** (trust any cert). Mutually exclusive with `usePlaintext`. |
| `batchSize`    | no       | `100`            | **Reserved.** Validated (`> 0`) but currently unused — size-based batching is disabled until the gateway supports it. |

The transport is one of three: **TLS with verification** (default), **plaintext** (`usePlaintext=true`),
or **insecure TLS** (`useInsecure=true`). Setting both `usePlaintext` and `useInsecure` is rejected.

Plus the standard Kafka Connect sink properties (`topics`, `tasks.max`, converters,
`errors.*`, …).

> **Secrets:** rather than inlining `token`, reference it through a
> [`config.providers`](https://kafka.apache.org/documentation/#connect_configproviders) provider
> configured on the Connect **worker** (e.g. the `FileConfigProvider`, as in the example below).
> The connector never logs the token or the config map.

### Example connector configuration

```json
{
  "name": "pinax-payment-gateway-usage-sink",
  "config": {
    "connector.class": "com.pinax.kafka.paymentgateway.connect.sink.PaymentGatewaySinkConnector",
    "tasks.max": "1",
    "topics": "metering-events",
    "endpoint": "abp.thegraph.market:443",
    "token": "${file:/secrets/payment-gateway.properties:token}",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

## Record format

- **Key** — ignored.
- **Value** — the protobuf-JSON encoding of a [`sf.metering.v1.Event`](src/main/proto/sf/metering/v1/metering.proto).
  Use a converter that yields the JSON **text** (e.g. `StringConverter`); the value is read via
  `value().toString()` and parsed with `JsonFormat`. Unknown fields are ignored.

| Field                | Type               | Notes                                                            |
|----------------------|--------------------|------------------------------------------------------------------|
| `user_id`            | string             | Events are grouped per `user_id`, one `ReportRequest` per user.  |
| `api_key_id`         | string             |                                                                  |
| `ip_address`         | string             |                                                                  |
| `endpoint`           | string             | The endpoint that emitted the event.                             |
| `network`            | string             | e.g. `eth-mainnet`.                                              |
| `meta` / `provider`  | string             |                                                                  |
| `output_module_hash` | string             |                                                                  |
| `metrics`            | array of `{key, value}` | `value` is a double.                                        |
| `timestamp`          | RFC 3339 timestamp | proto3 JSON timestamp mapping.                                   |

The field names above are the proto names. Protobuf's `JsonFormat` parser accepts **both** these
names (`user_id`) and their proto3 lowerCamelCase form (`userId`), so either works in the value:

```json
{
  "user_id": "user-123",
  "api_key_id": "key-abc",
  "network": "eth-mainnet",
  "endpoint": "sf.firehose.v2.Stream/Blocks",
  "metrics": [{ "key": "egress_bytes", "value": 4096 }],
  "timestamp": "2026-06-03T12:00:00Z"
}
```

## Behaviour & delivery semantics

- **At-least-once.** On a `RetriableException`, Connect re-delivers the whole batch. Because a
  batch is reported per user, if reporting fails partway through, the users already reported are
  reported **again** on retry. This is accepted in exchange for never dropping usage.
- **Error classification** (`report()` maps failures to the right Connect signal):

  | Failure | Mapped to | Effect |
  |---|---|---|
  | gRPC `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNKNOWN`, `UNAUTHENTICATED`, `PERMISSION_DENIED` | `RetriableException` | Connect retries the batch |
  | Other gRPC status (`INVALID_ARGUMENT`, `NOT_FOUND`, …) | `DataException` | Non-retriable |
  | Malformed event JSON (`InvalidProtocolBufferException`) | `DataException` | Non-retriable |
  | `null` record value | `DataException` | Non-retriable |
  | Invalid client TLS setup | `ConnectException` | Task fails fast (permanent misconfig) |

- **Connection lifecycle.** gRPC connects lazily, so a transient gateway outage at startup is
  tolerated. A failed batch drops the channel; the next `put()` rebuilds it from a clean state.

## Testing

```bash
mvn test
```

## Continuous integration

[`.github/workflows/upload-jar-on-release.yaml`](.github/workflows/upload-jar-on-release.yaml)
builds the jar on a GitHub Release, attaches it to the release, and dispatches a build event to
`pinax-network/pinax-connect`.
