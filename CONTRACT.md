# Prism / Polarizer Kafka Contract

Polarizer is the only production producer for `prism.stream.jobs`. Values are raw binary Protobuf and CloudEvents metadata is stored in Kafka headers. Kafka topic ACLs must grant job-topic write access only to Polarizer's authenticated principal and read access only to Prism. Prism additionally rejects records whose declared source or type does not match its configured Polarizer identity.

## Enqueueing Work (Input Stream)

Polarizer publishes `PrismStreamPayload` directly to `prism.stream.jobs`. Confluent framing, JSON envelopes, and externally written Redis stream entries are not accepted production contracts.

Required Kafka headers are `ce_specversion=1.0`, `ce_type=fun.interchat.prism.job`, `ce_source=/polarizer`, `ce_id`, `ce_time`, `ce_datacontenttype=application/protobuf`, and `content-type=application/protobuf`. Producers should set a non-empty Kafka key; for compatibility, Prism accepts an empty key from an otherwise valid Polarizer envelope and uses the authenticated action ID when publishing a retry. `PRISM_JOB_SOURCE` and `PRISM_JOB_EVENT_TYPE` may tighten the expected identity/type but must agree with Polarizer.

Production delivery is synchronous with the Kafka acknowledgement boundary: Prism does not return the Broadway message until Discord processing has completed. Completion callbacks are attempted with bounded retries, but a callback outage never replays completed Discord side effects. Broadway Kafka commits failed records, so `handle_failed/2` first publishes retriable raw jobs to `PRISM_JOBS_RETRY_TOPIC` and waits for broker acknowledgement. Whole-job retries are capped by `EVENT_BUS_MAX_RETRIES` and then sent to the restricted `PRISM_JOBS_DLQ_TOPIC`; invalid contracts go directly to that DLQ. Mandatory Kafka retry/DLQ handoffs use bounded exponential backoff until the broker acknowledges them.

Retry values are the exact original Protobuf bytes with the original CloudEvents headers and partition key. Additional headers record `prism-original-topic`, monotonically increasing `prism-retry-attempt`, `prism-not-before-ms`, and a typed `prism-retry-reason`. Prism consumes both the primary and retry topics. A retry record remains retained and unacknowledged while its not-before deadline is pending; Redis may still accelerate per-target scheduling but is never the authoritative copy of a whole approved job.

### Payload Schema (Protobuf + JSON)

The outer envelope is a Protobuf message for performance, but the inner `payload` remains a stringified JSON object containing the standard Discord fields.

```protobuf
message PrismStreamMetadata {
  string author_id = 1;
  string guild_id = 2;
  string guild_name = 3;
  repeated string badges = 4;
}

message PrismTarget {
  string channel_id = 1;
  string webhook_id = 2;
  string webhook_token = 3;
  optional string guild_id = 4;
  optional string hub_id = 5;
  optional string thread_id = 6;
  optional string message_id = 7;
  optional string overrides = 8; // JSON string
}

message PrismStreamPayload {
  string batch_id = 1;
  string action = 2;
  optional string message_id = 3;
  optional int32 shard_index = 4;
  optional string hub_id = 5;
  optional string hub_name = 6;
  string payload = 7; // JSON string of Discord payload
  repeated PrismTarget targets = 8;
  optional PrismStreamMetadata metadata = 9;
  optional string action_id = 10;
}
```

Prism requires a UUIDv7 `action_id`, non-empty `batch_id` and `message_id`, and at least one target. These identities survive internal delay/retry processing. Authoritative delivery receipts are keyed by `action_id`; observational batch callbacks retain `batch_id` and parent message identity.

```protobuf
message PrismDeliveryCallback {
  string action_id = 1;
  string message_id = 2;
  MessageState state = 3; // ACTIVE or DELIVERY_FAILED
  string failure_code = 4;
  google.protobuf.Timestamp occurred_at = 5;
}
```

Prism publishes this raw message to `events.prism.delivery.v2`, keyed by `action_id`, only after Discord delivery succeeds or the batch has no successful target. Polarizer consumes it to transition `APPROVED_PENDING_DELIVERY` to `ACTIVE` or `DELIVERY_FAILED`. A later successful retry may transition `DELIVERY_FAILED` to `ACTIVE`. Observational summaries retain `action_id`, `batch_id`, and `parent_message_id` for bot correlation but are not authoritative.

Per-target Redis checkpoints are keyed by Polarizer action ID, batch ID, and webhook target ID. A replay after callback publication therefore reuses completed target results and republishes the idempotent state callback instead of intentionally issuing the Discord request again. There remains an unavoidable external-side-effect ambiguity if the process dies after Discord accepts a webhook but before the checkpoint write; staging fault-injection tests must exercise this window before promotion.

Example of the inner JSON `payload` string:
```json
{
  "batch_id": "string (unique identifier for this batch)",
  "action": "execute | edit | delete",
  "message_id": "string (optional, used as parent_message_id in callbacks for execute actions)",
  "metadata": {
    "any_key": "any_value"
  },
  "payload": {
    "content": "Hello World",
    "embeds": [],
    "components": [],
    "allowed_mentions": { "parse": [] }
  },
  "targets": [
    {
      "webhook_id": "1234567890",
      "webhook_token": "abc_xyz...",
      "thread_id": "string (optional)",
      "message_id": "string (required if action is edit/delete)",
      "overrides": {
        "content": "Hello <@123456>!",
        "allowed_mentions": { "users": ["123456"] }
      },
      "channel_id": "string",
      "guild_id": "string",
      "connection_id": "string",
      "hub_id": "string"
    }
  ]
}
```

### Supported Actions
- `execute`: Creates a new webhook message (POST)
- `edit`: Edits an existing webhook message (PATCH). Requires `message_id` on each target.
- `delete`: Deletes an existing webhook message (DELETE). Requires `message_id` on each target.

### Overrides Mechanism

Prism takes the base `payload` and does a shallow merge with a target's `overrides` dictionary before executing the HTTP request. This allows you to efficiently send identical messages to most targets while sending slight variations (e.g., custom pings or components) to specific targets within the same batch.

### Wire Format (Key Minification)

To reduce Redis stream memory usage, publishers may minify JSON keys to 1–2 character codes. Prism automatically expands them back to full names before processing. The key mapping is controlled by the `@key_map` in `Prism.FanoutBroadway.KeyExpansion`. If you do not use key minification, set `PRISM_KEY_EXPANSION_ENABLED=false` to skip this step.

| Long key | Short key | Level |
|---|---|---|
| `action` | `a` | root |
| `batch_id` | `b` | root |
| `message_id` | `m` | root / target |
| `shard_index` | `s` | root |
| `hub_id` | `h` | root / target |
| `hub_name` | `n` | root |
| `payload` | `p` | root |
| `targets` | `t` | root |
| `metadata` | `d` | root |
| `trace_headers` | `r` | root |
| `channel_id` | `c` | target |
| `webhook_id` | `w` | target |
| `webhook_token` | `k` | target |
| `guild_id` | `g` | target / metadata |
| `thread_id` | `f` | target |
| `overrides` | `o` | target |
| `connection_id` | `ci` | target |
| `username` | `u` | payload |
| `avatar_url` | `v` | payload |
| `content` | `x` | payload |
| `embeds` | `e` | payload |
| `components` | `q` | payload |
| `allowed_mentions` | `l` | payload |
| `flags` | `fl` | payload |
| `author_id` | `ai` | metadata |
| `guild_name` | `gn` | metadata |
| `badges` | `bg` | metadata |

Short keys that appear at multiple nesting levels (e.g. `m` for `message_id` at root and target level) are safe because JSON keys are scoped to their parent object.

---

## Callbacks (Output Stream)

Once a batch is fully processed (or hits maximum retries), Prism writes a CloudEvent callback to the centralized EventBus stream (configurable via `EVENTS_STREAM`, default: `events:bus`).

### Callback Schema

```json
{
  "batch_id": "string (matches input)",
  "action": "execute | edit | delete",
  "status": "success | partial_retry",
  "parent_message_id": "string (matches the message_id passed in the root of the input payload)",
  "message_ids": [
    {
      "webhook_id": "1234567890",
      "message_id": "9876543210",
      "channel_id": "string",
      "guild_id": "string",
      "connection_id": "string",
      "hub_id": "string"
    }
  ],
  "failures": [
    {
      "webhook_id": "1234567890",
      "error": "rate_limited | invalid_webhook | message_not_found | bad_request | server_error | network_error | permanent_error",
      "error_type": "transient | permanent",
      "channel_id": "string",
      "guild_id": "string"
    }
  ]
}
```

### Error Types

| Error | Type | Meaning |
|---|---|---|
| `rate_limited` | transient | Discord returned 429. Prism retries automatically after the `retry_after` delay. |
| `server_error` | transient | Discord returned 5xx. Prism retries with exponential backoff up to the configured max attempts. |
| `network_error` | transient | TCP connection failure, DNS error, or unrecognized 404. Prism retries. |
| `message_not_found` | permanent | Discord returned 10008 (unknown message). The target message was deleted. |
| `invalid_webhook` | permanent | Discord returned 10003 or 10015. The webhook URL is invalid or deleted. |
| `permanent_error` | permanent | Discord returned 401 or 403. The token is invalid or missing permissions. |
| `bad_request` | transient | Discord returned 400. Typically a malformed embed or payload; not retried by default. |

---

## Configurable Stream Keys

All stream keys and Redis identifiers are configurable. See `.env.example` for the full list of environment variables and their defaults.

| Config Key | Env Var | Default |
|---|---|---|
| Jobs lane stream | `PRISM_STREAM_JOBS` | `prism.stream.jobs` |
| Retry stream | `REDIS_RETRY_STREAM` | `prism.stream.retries` |
| EventBus stream (Callbacks) | `EVENTS_STREAM` | `events:bus` |
| Consumer group | `PRISM_CONSUMER_GROUP` | `prism:cg:fanout` |
| Delayed queue ZSET | `PRISM_DELAYED_ZSET_KEY` | `prism:delayed` |
| PubSub wakeup channel | `PRISM_PUBSUB_CHANNEL` | `prism:wakeup` |

---

## Event Bus Output (CloudEvents v1.0)

Prism publishes observational JSON CloudEvents to the shared Kafka event topic. The primary topic is `events.bus` with a restricted dead-letter topic at `events.bus.dlq`. These summaries do not drive authoritative message state; `PrismDeliveryCallback` on `events.prism.delivery.v2` does.

### CloudEvent Envelope Schema

Every event is wrapped in a CloudEvents v1.0 envelope:

```json
{
  "specversion": "1.0",
  "type": "fun.interchat.broadcast.completed",
  "source": "/prism",
  "id": "evt_<hex>",
  "time": "2026-06-24T09:30:00Z",
  "datacontenttype": "application/json",
  "data": { ... },
  "traceparent": "00-<trace_id>-<span_id>-01",
  "tracestate": null
}
```

| Field | Type | Description |
|---|---|---|
| `specversion` | string | Always `"1.0"` |
| `type` | string | Reverse-DNS event type, e.g. `fun.interchat.broadcast.completed` |
| `source` | string | URI identifying the producing service, e.g. `"/prism"` |
| `id` | string | Unique event ID prefixed with `evt_` |
| `time` | string | RFC 3339 timestamp |
| `datacontenttype` | string | Always `"application/json"` |
| `data` | object | Event-specific payload |
| `traceparent` | string | W3C trace context propagation header |
| `tracestate` | string | W3C tracestate header (omitted when null) |

### Event Type Catalog

| Event Type | Source | Data Schema | Consumers |
|---|---|---|---|
| `fun.interchat.broadcast.completed` | `/prism` | `batch_id`, `action`, `ok_count`, `fail_count`, `parent_message_id`, `hub_id`, `timestamp` | Beacon (hub fanout) |

*Note: Prism is primarily an event producer. Event types consumed by Beacon and Bot are documented in their respective CONTRACT files.*

### Event Bus DLQ

Failed events that exhaust all retry attempts are written to `events.bus.dlq` (configurable via `EVENTS_DLQ_STREAM`):

```json
{
  "original_event": { ... },
  "error": "descriptive error message",
  "failed_at": "2026-06-24T09:30:00Z",
  "attempts": 3,
  "consumer_group": "prism-cg:..."
}
```

| Field | Type | Description |
|---|---|---|
| `original_event` | object | The full CloudEvent that failed |
| `error` | string | Error message or exception |
| `failed_at` | string | ISO 8601 timestamp of final failure |
| `attempts` | integer | Number of delivery attempts made |
| `consumer_group` | string | Consumer group that exhausted retries |

### Consumer Group Model

Each service creates its own consumer groups on the shared `events.bus` stream. Consumer groups follow the pattern `<service>-cg:<purpose>`:

| Service | Consumer Group Pattern | Purpose |
|---|---|---|
| Beacon | `beacon-cg:hub-fanout` | Hub message dispatch |
| Bot | `bot-cg:cache-invalidator` | Cross-shard cache invalidation |

Per-shard consumer names (`<service>-<shard_index>`) ensure at-least-once delivery and enable stale message recovery via XAUTOCLAIM.

### Transport Abstraction

The EventBus adapter supports pluggable transport backends behind a unified contract:

| Language | Contract Type | Implementation |
|---|---|---|
| Elixir | `@behaviour` (`Prism.EventBus.Transport.Behaviour`) | `Prism.EventBus.Transport.Redis` |
| Python | `Protocol` (`EventBusTransport`) | `RedisStreamTransport` |
| Go | `interface` (`Publisher`) | `RedisPublisher` |
| Rust | `trait` (`EventBus`) | `RedisEventBus` |

Production sets `EVENT_BUS_TRANSPORT=kafka`; any other runtime value is rejected. The Redis transport remains available only to isolated tests, while Redis-backed delayed retries are internal Prism state rather than an external ingestion path.
