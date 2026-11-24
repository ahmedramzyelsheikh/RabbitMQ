# 03 · Publish / Subscribe (fanout)

Broadcast every message to every subscriber.

```mermaid
flowchart LR
    P[publisher] --> X{{logs<br/>fanout}}
    X --> Q1[(amq.gen-1)] --> S1[console-printer]
    X --> Q2[(amq.gen-2)] --> S2[file-writer]
    X --> Q3[(amq.gen-3)] --> S3[metrics]
```

## Run

```bash
dotnet run --project samples/03-PublishSubscribe
dotnet run --project samples/03-PublishSubscribe -- subscribe screen
dotnet run --project samples/03-PublishSubscribe -- publish "error: disk full"
```

## What to notice

* Step 1 publishes before anyone is bound: the message is **dropped** (an exchange is not storage).
* Each subscriber declares `queue: ""`, `exclusive: true`, `autoDelete: true` → server-named temporary queue that
  disappears with the subscriber. Fine for live views; services that must not miss events use durable named
  queues (sample 05).
* 3 subscribers × 3 messages = 9 deliveries.

Theory: [03 Exchanges and routing](../../docs/03-exchanges-and-routing.md).
