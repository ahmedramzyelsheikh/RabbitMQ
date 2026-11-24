# 13 · Interview questions and exercises

---

## Part A — Interview questions (with short answers)

### Fundamentals

1. **What is RabbitMQ and why use a broker instead of HTTP calls?**
   A message broker that stores and routes messages. It decouples services in time and space, levels load,
   allows fan-out and retries, and keeps working when consumers are down. HTTP is better when the caller needs an
   immediate answer.
2. **Exchange vs queue vs binding?** Exchanges route, queues store, bindings are the routing rules between them.
3. **Do producers publish to queues?** No, always to an exchange; "publish to a queue" uses the default exchange
   with the queue name as routing key.
4. **Connection vs channel?** A connection is a TCP socket; channels are lightweight virtual connections inside it.
   One connection per app, one channel per thread/consumer.
5. **Name the exchange types.** Default, direct, fanout, topic, headers (+ plugins such as consistent-hash).
6. **`*` vs `#` in topic bindings?** `*` exactly one word, `#` zero or more words.
7. **What happens to a message no binding matches?** Dropped (and still confirmed) unless `mandatory` (returned
   with 312 NO_ROUTE) or an alternate exchange is configured.
8. **What is a vhost?** An isolated namespace of exchanges/queues/permissions inside one broker.

### Reliability

9. **How do you make sure a message survives a broker restart?** Durable exchange, durable (quorum) queue,
   persistent message, publisher confirms.
10. **What are publisher confirms?** Broker acks/nacks for each published message; the publisher knows it was
    stored.
11. **autoAck true vs false?** True = at-most-once (deleted on delivery). False = at-least-once (deleted after
    the consumer acks; redelivered if the consumer dies).
12. **What is prefetch and how do you choose it?** Max unacked messages per consumer; 1 for long tasks/fairness,
    tens to hundreds for fast handlers. Never unlimited.
13. **Can RabbitMQ deliver exactly once?** No. At-least-once + idempotent consumers = effectively once.
14. **What is the dual-write problem and how do you solve it?** Saving to a DB and publishing are two systems; a
    crash between them loses or invents events. Transactional outbox (or CDC).
15. **How do you make consumers idempotent?** Inbox table keyed by MessageId, unique business keys, naturally
    idempotent updates, version checks, idempotency keys for external APIs.
16. **What does `Redelivered = true` mean?** The message was delivered before and not acked (consumer died,
    nack/requeue). It may or may not have been processed.

### Failures

17. **What is a dead letter exchange and when are messages dead-lettered?** An exchange that receives messages
    that were rejected without requeue, expired, overflowed, or exceeded a quorum delivery limit.
18. **How do you implement delayed retry without plugins?** Nack to a DLX that routes to a retry queue with
    `x-message-ttl`, whose DLX routes back to the work exchange; count attempts with `x-death`.
19. **What is a poison message and how do you stop the loop?** A message that always fails. Don't requeue
    forever: delayed retries with a maximum, parking lot, quorum `x-delivery-limit`.
20. **What happens if a consumer never acks?** Messages stay unacked, the consumer stops getting more once
    prefetch is full, and after `consumer_timeout` the broker closes the channel.

### Queues and scaling

21. **Classic vs quorum vs stream queues?** Classic: single node, temporary/exclusive use. Quorum: replicated
    with Raft, for important data, delivery limit. Stream: append-only log, replay from offsets.
22. **How do you scale consumers? What about ordering?** Competing consumers on one queue; ordering only holds
    with one consumer. For order per key use single active consumer or sharding.
23. **Why should queues be short?** Long queues use memory/disk, slow down restarts and signal that consumers
    can't keep up.
24. **What happens on a memory alarm?** All publishing connections are blocked until memory drops; consumers
    continue.
25. **How do you change the arguments of an existing queue?** You can't (406). Create a new queue, or use
    policies for tunable settings.

### Design

26. **Command vs event?** Command: one handler, imperative ("SendInvoice"). Event: many subscribers, past tense
    ("OrderPlaced").
27. **How does RPC over RabbitMQ work?** ReplyTo + CorrelationId, reply queue or direct reply-to, timeouts.
28. **RabbitMQ vs Kafka?** Broker with queues and rich routing, deleted on ack, vs distributed log with retention
    and replay by offset. Tasks/integration vs streaming/analytics.
29. **Choreography vs orchestration sagas?** Services react to each other's events vs a coordinator sending
    commands and tracking state.
30. **How do you send a 500 MB file?** You don't: store it in blob storage and send a reference (claim check).

### .NET specifics

31. **What changed in RabbitMQ.Client 7?** Fully async API (`IChannel`, `CreateChannelAsync`,
    `BasicPublishAsync`), `BasicProperties` class, `AsyncEventingBasicConsumer`, confirmations configured via
    `CreateChannelOptions` with `BasicPublishAsync` awaiting the confirm.
32. **How do you host a consumer in ASP.NET Core?** `BackgroundService` with its own channel, singleton
    `IConnection`, graceful `StopAsync`, health check.
33. **Raw client or MassTransit?** Raw client for control/learning/simple cases; a framework for many message
    types, retries, outbox, sagas. Mind MassTransit v9's commercial licence.
34. **How do you test RabbitMQ code?** Unit-test handlers without the broker; integration-test with
    Testcontainers.RabbitMq (this repo's `tests/LearningTests`).

---

## Part B — Exercises

Each exercise builds on a sample. Run the broker with `docker compose up -d` first.

### Beginner

1. **Hello again** (sample 01): run `receive` in one terminal and `send hi there` in another. Stop the receiver,
   send 3 messages, look at the queue in the UI, start the receiver again.
2. **Lose a message on purpose** (sample 01): set `autoAck: true`, add `Environment.Exit(1)` in the handler, send a
   message and observe it is gone. Switch to manual acks and repeat.
3. **Fair dispatch** (sample 02): run two `worker`s and `send 20`. Remove `BasicQosAsync` and compare the
   distribution.
4. **Log router** (sample 04): add a `critical` severity that only a new `pager` subscriber receives.

### Intermediate

5. **Topic design**: design routing keys for a fleet system (`vehicle.<id>.position`, `vehicle.<id>.fuel.low`,
   `driver.<id>.shift.started`) and bindings for: live map (positions only), alerts (anything ending `low` or
   `failed`), audit (everything). Prove it with a test in `RoutingTests`.
6. **RPC timeout** (sample 07): make the server sleep 6 s for country `XX` and handle the timeout in the client
   with a fallback price.
7. **Confirm throughput** (sample 08): measure batch sizes 10, 100, 500, 1000 and plot the time.
8. **Exponential retry** (sample 09): replace the single 2 s retry queue with tiers 1 s, 5 s, 15 s.
9. **Priority lanes**: implement urgent/bulk email with two queues and compare with `x-max-priority`.

### Advanced

10. **Outbox with PostgreSQL**: port sample 12 to PostgreSQL and run two dispatcher instances safely with
    `FOR UPDATE SKIP LOCKED`.
11. **Saga**: orchestrate `OrderPlaced → ChargeCard → ReserveStock → ScheduleShipment` with compensation
    (`RefundPayment`) using plain RabbitMQ.Client and a saga table, then again with a MassTransit state machine.
12. **Ordering per key**: enable the `rabbitmq_consistent_hash_exchange` plugin and keep per-vehicle order with 4
    queues and 4 consumers.
13. **Observability**: add `RabbitMQ.Client.OpenTelemetry` to sample 13 and see one trace from HTTP request to
    consumer in the Aspire dashboard or Jaeger.
14. **Chaos**: while sample 02 workers run, `docker restart rabbitmq-learning`. Watch automatic recovery and
    check whether any task was lost or duplicated. Then run a 3-node cluster with docker compose and kill the
    quorum queue leader.
15. **Streams**: build a "price history" service that rebuilds its in-memory state on startup by replaying a
    stream from `first`, and stores its last processed offset to resume.

---

⬅️ Back to the [learning path](README.md)
