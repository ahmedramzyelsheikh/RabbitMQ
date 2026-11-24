# 11 · Streams

An append-only, replayable log inside RabbitMQ, consumed over plain AMQP.

```mermaid
flowchart LR
    P[publisher] --> S[(events.stream<br/>0 1 2 3 4 5 6 7 8 9 10 11)]
    S -- "offset first" --> R[reporting: 0..11]
    S -- "offset 5" --> A[analytics: 5..11]
    S -- "offset next" --> L[live-dashboard: 10, 11]
    S -- "offset first, later" --> J[late-joiner: 0..11]
```

## Run

```bash
dotnet run --project samples/11-Streams
```

## What to notice

* Declared with `x-queue-type = stream` and retention `x-max-age = 7D`.
* Consumers **must** set prefetch and use manual acks; acks give credit, they don't delete.
* `x-stream-offset` accepts `first`, `last`, `next`, a numeric offset, a timestamp or an interval like `"10m"`.
* Every delivery carries its position in the `x-stream-offset` header.
* For very high throughput use the native stream protocol (`RabbitMQ.Stream.Client`).

Theory: [06 Queue types](../../docs/06-queue-types-and-arguments.md),
[12 RabbitMQ vs Kafka](../../docs/12-rabbitmq-vs-others.md).
