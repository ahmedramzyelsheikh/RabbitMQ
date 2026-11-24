# RabbitMQ with C# — from scratch to advanced

A study repository for RabbitMQ in .NET 10 / C#: theory with workflow diagrams, when to use it and when not,
how it compares to traditional integration, the NuGet packages you need, and runnable samples from Hello World
to outbox/inbox, an ASP.NET Core service and MassTransit. Every broker behaviour is also proven by learning tests
that run against a real RabbitMQ in a container.

## Requirements

- .NET SDK 10
- Docker (for the broker and for the tests)

## Quick start

```bash
docker compose up -d                                   # RabbitMQ 4.2 + management UI
dotnet build
dotnet run --project samples/01-HelloWorld             # any sample, see the table below
dotnet test                                            # learning tests (Testcontainers starts its own broker)
```

Management UI: http://localhost:15672 (guest / guest). Set `RABBITMQ_URI` to use another broker,
for example `amqp://user:pass@host:5672/vhost`.

## Samples

| # | Sample | What it teaches |
|---|--------|-----------------|
| 01 | [HelloWorld](samples/01-HelloWorld) | connection, channel, queue declare, default exchange, publish, consume |
| 02 | [WorkQueues](samples/02-WorkQueues) | competing consumers, manual ack, redelivery after a crash, prefetch / fair dispatch, persistence |
| 03 | [PublishSubscribe](samples/03-PublishSubscribe) | fanout exchange, server-named exclusive queues, messages lost when nobody is bound |
| 04 | [Routing](samples/04-Routing) | direct exchange, several binding keys per queue |
| 05 | [Topics](samples/05-Topics) | topic exchange, `*` and `#` wildcards, durable service queues |
| 06 | [Headers](samples/06-Headers) | headers exchange, `x-match` all / any |
| 07 | [Rpc](samples/07-Rpc) | request/reply, `replyTo` + `correlationId`, direct reply-to, timeouts, consumer concurrency |
| 08 | [PublisherConfirms](samples/08-PublisherConfirms) | confirms one-by-one vs batched, mandatory + returns, alternate exchange, channel errors |
| 09 | [DeadLetterRetry](samples/09-DeadLetterRetry) | dead letter exchange, delayed retry with a TTL queue, `x-death`, parking lot |
| 10 | [QueueFeatures](samples/10-QueueFeatures) | TTL, expiration, max-length + overflow, priority, `x-expires`, quorum delivery-limit, single active consumer |
| 11 | [Streams](samples/11-Streams) | stream queues, offsets, replay, non-destructive reads |
| 12 | [OutboxInbox](samples/12-OutboxInbox) | transactional outbox with EF Core, idempotent consumer / inbox, duplicates on the wire |
| 13 | [OrdersApi](samples/13-OrdersApi) | ASP.NET Core minimal API publishing events, `BackgroundService` consumer, DI, health |
| 14 | [MassTransit](samples/14-MassTransit) | the same ideas through a framework: publish, send, request/response, retries, faults |

Most samples run a self-contained demo by default. Several also accept roles so you can run them in separate
terminals, for example:

```bash
dotnet run --project samples/02-WorkQueues -- worker A
dotnet run --project samples/02-WorkQueues -- worker B
dotnet run --project samples/02-WorkQueues -- send 20
```

## Learning tests

[tests/LearningTests](tests/LearningTests) proves each rule against a real broker: routing for every exchange
type, mandatory/returns, alternate exchanges, ack/nack/reject, prefetch, competing consumers, dead-letter reasons
(`rejected`, `expired`, `maxlen`, `delivery_limit`), delayed retry, priority, overflow, argument conflicts (406),
exclusive queues (405), single active consumer and stream replay.
