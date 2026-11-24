# 02 · Core concepts and the AMQP model

> How RabbitMQ is built: broker, virtual hosts, connections, channels, exchanges, queues, bindings, the message
> itself, the protocols and ports, and the life of one message from publish to ack.

---

## 1. The big picture

```mermaid
flowchart LR
    subgraph App1[Producer app]
        P[Publisher code] --> CH1[Channel]
    end
    subgraph Broker[RabbitMQ node]
        subgraph VH[virtual host '/']
            X{{Exchange}} -- binding key --> Q[(Queue)]
        end
    end
    subgraph App2[Consumer app]
        CH2[Channel] --> C[Consumer code]
    end
    CH1 == TCP connection ==> X
    Q == TCP connection ==> CH2
```

**Producers never publish to a queue directly. They publish to an exchange**, and the exchange decides which
queue(s) get a copy, using bindings. Even "publish to queue X" goes through the **default exchange** (doc 03).

---

## 2. The building blocks

| Concept | What it is | .NET (RabbitMQ.Client 7) |
|--------|-----------|--------------------------|
| **Node / cluster** | one RabbitMQ server / several joined together | – |
| **Virtual host (vhost)** | isolated namespace: own exchanges, queues, users' permissions (like a database inside a DB server) | `ConnectionFactory.VirtualHost` or the URI path |
| **Connection** | one long-lived TCP (optionally TLS) connection, expensive to open | `IConnection` from `factory.CreateConnectionAsync()` |
| **Channel** | lightweight virtual connection multiplexed inside a connection; all operations happen on a channel | `IChannel` from `connection.CreateChannelAsync()` |
| **Exchange** | router: `direct`, `fanout`, `topic`, `headers` | `ExchangeDeclareAsync` |
| **Queue** | ordered buffer (FIFO) that stores messages; types: classic, quorum, stream | `QueueDeclareAsync` |
| **Binding** | exchange → queue (or exchange → exchange) rule with a binding key or header arguments | `QueueBindAsync`, `ExchangeBindAsync` |
| **Consumer** | subscription on a queue, the broker **pushes** messages | `BasicConsumeAsync` + `AsyncEventingBasicConsumer` |
| **Message** | properties + headers + body (bytes) | `BasicProperties` + `ReadOnlyMemory<byte>` |

### Connection vs channel

```mermaid
flowchart TB
    subgraph Process[Your .NET process]
        CONN[IConnection - 1 TCP socket]
        CONN --> C1[IChannel 1: publisher]
        CONN --> C2[IChannel 2: consumer of orders]
        CONN --> C3[IChannel 3: consumer of emails]
    end
    CONN === TCP[(TCP 5672)] === RMQ[RabbitMQ]
```

* **One connection per process** (sometimes two: one for publishing, one for consuming so a blocked publisher
  doesn't stop consumers).
* **Many channels**, one per concern/thread. Channels are cheap but not free; don't open one per message.
* A protocol error (e.g. declaring a queue with different arguments) **closes the channel**, not the connection.

---

## 3. Anatomy of a message

```mermaid
flowchart LR
    subgraph Message
        direction TB
        RK[routing key: order.eg.created<br/>exchange: shop.events]
        PR[properties:<br/>message-id, correlation-id, reply-to,<br/>content-type, delivery-mode,<br/>priority, expiration, timestamp, type, app-id]
        HD[headers: tenant=acme, x-retry=2, ...]
        BD[body: bytes - JSON, Protobuf, ...]
    end
```

| Property | Use it for | Example |
|----------|-----------|---------|
| `MessageId` | unique id → de-duplication (inbox) | `Guid.NewGuid().ToString()` |
| `CorrelationId` | link a reply/event to the request/flow | RPC, tracing |
| `ReplyTo` | where to send the answer | `amq.rabbitmq.reply-to` |
| `ContentType` / `ContentEncoding` | how to read the body | `application/json` |
| `DeliveryMode` / `Persistent` | `true` = written to disk in durable queues | `Persistent = true` |
| `Priority` | 0–255 (use 0–10) for priority queues | `Priority = 9` |
| `Expiration` | per-message TTL in ms, **a string** | `"60000"` |
| `Timestamp` | when it was created (Unix seconds) | `new AmqpTimestamp(...)` |
| `Type` | message type name | `"OrderPlaced"` |
| `AppId`, `UserId` | who sent it (`UserId` is validated by the broker) | `"orders-api"` |
| `Headers` | anything custom; also used by headers exchanges and the broker (`x-death`, `x-delivery-count`) | `["tenant"] = "acme"` |

> The body is just **bytes**. RabbitMQ never looks inside it. Choose a format (JSON is the usual default) and
> put the type/version in `Type` or a header so consumers can evolve.

---

## 4. Life of a message

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant X as Exchange
    participant Q as Queue
    participant C as Consumer
    P->>X: basic.publish(exchange, routingKey, props, body)
    X->>X: match bindings
    alt at least one binding matches
        X->>Q: enqueue a copy (one per matching queue)
        X-->>P: confirm ack (if confirms enabled)
    else nothing matches
        X-->>P: basic.return (only if mandatory=true)
        X-->>X: otherwise dropped / alternate exchange
    end
    Q->>C: basic.deliver (push, up to prefetch unacked)
    C->>C: process
    alt success
        C->>Q: basic.ack -> message deleted
    else failure
        C->>Q: basic.nack / reject (requeue or dead-letter)
    else consumer dies (no ack)
        Q->>Q: message goes back to the queue, redelivered=true
    end
```

---

## 5. Declaring topology

Declarations are **idempotent**: declaring something that already exists with the **same** settings is a no-op;
with **different** settings the broker answers `406 PRECONDITION_FAILED` and closes the channel.

```csharp
await channel.ExchangeDeclareAsync("shop.events", ExchangeType.Topic, durable: true, autoDelete: false);

await channel.QueueDeclareAsync("email.order-placed", durable: true, exclusive: false, autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        ["x-queue-type"] = "quorum",
        ["x-dead-letter-exchange"] = "shop.dead-letter"
    });

await channel.QueueBindAsync("email.order-placed", "shop.events", routingKey: "order.placed");
```

Who declares? Common rule: **consumers declare their queues and bindings; producers declare the exchanges they
publish to.** Both sides declaring is fine as long as the settings match. In bigger systems topology is often
created by a deployment script / definitions file instead (doc 11).

| Flag | Queue meaning |
|------|---------------|
| `durable` | the queue definition survives a broker restart (quorum and stream queues are always durable) |
| `exclusive` | only this connection can use it; deleted when the connection closes |
| `autoDelete` | deleted when the last consumer unsubscribes |
| `queue: ""` | the broker generates a name (`amq.gen-...`), perfect for temporary reply/subscriber queues |
| `passive: true` | only check that it exists (404 if not) |

> **Durable queue + persistent message** = survives a restart. A durable queue with transient messages loses the
> messages; a transient queue loses everything.

---

## 6. Protocols and ports

| Port | Protocol / use |
|------|----------------|
| 5672 / 5671 | **AMQP 0-9-1** (this repo, RabbitMQ.Client) and AMQP 1.0 (native in RabbitMQ 4), 5671 = TLS |
| 15672 | management UI + HTTP API |
| 5552 / 5551 | RabbitMQ **Stream** protocol (high-throughput stream clients) |
| 1883 / 8883 | MQTT (IoT devices, plugin) |
| 61613 | STOMP (plugin) |
| 15692 | Prometheus metrics (plugin) |
| 25672, 4369 | inter-node clustering / Erlang port mapper |

AMQP 0-9-1 is the classic RabbitMQ protocol and what most .NET code uses. RabbitMQ 4.x also speaks AMQP 1.0
natively (the `RabbitMQ.AMQP.Client` package), which is the same idea with a different wire protocol.

---

## 7. The management UI and CLI

* **UI** http://localhost:15672: overview (message rates), connections, channels, exchanges, queues (get/publish
  messages manually, purge, delete), admin (users, vhosts, policies).
* **CLI** inside the container:

```bash
docker exec rabbitmq-learning rabbitmqctl list_queues name type messages consumers
docker exec rabbitmq-learning rabbitmqctl list_exchanges name type
docker exec rabbitmq-learning rabbitmqctl list_bindings
docker exec rabbitmq-learning rabbitmq-diagnostics status
docker exec rabbitmq-learning rabbitmqadmin list queues   # HTTP API client
```

## 8. Your first connection in C#

```csharp
var factory = new ConnectionFactory
{
    Uri = new Uri("amqp://guest:guest@localhost:5672/"),
    ClientProvidedName = "orders-api",              // shows in the UI, invaluable when debugging
    AutomaticRecoveryEnabled = true,                // reconnect + recover channels/consumers (default true)
    TopologyRecoveryEnabled = true,
    RequestedHeartbeat = TimeSpan.FromSeconds(30)
};

await using IConnection connection = await factory.CreateConnectionAsync();
await using IChannel channel = await connection.CreateChannelAsync();
```

Run [samples/01-HelloWorld](../samples/01-HelloWorld) and watch the queue in the UI.

➡️ Next: [03 · Exchanges and routing](03-exchanges-and-routing.md)
