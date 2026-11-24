using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string ExchangeName = "orders.topic";

Log.Title("05 Topics: topic exchange, '*' = exactly one word, '#' = zero or more words");

await using var connection = await Broker.ConnectAsync("05-topics");
await using var publisher = await connection.CreateChannelAsync();
await publisher.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Topic, durable: true, autoDelete: false);

(string Service, string Queue, string[] Patterns)[] services =
[
    ("billing", "billing.orders-created", ["order.*.created"]),
    ("egypt-ops", "egypt-ops.orders", ["order.eg.#"]),
    ("failures", "alerts.failures", ["#.failed"]),
    ("audit", "audit.everything", ["#"])
];

(string RoutingKey, string Text)[] events =
[
    ("order.eg.created", "order 1 created in Egypt"),
    ("order.sa.created", "order 2 created in Saudi Arabia"),
    ("order.eg.cancelled", "order 3 cancelled in Egypt"),
    ("order.eg.payment.failed", "order 4 payment failed in Egypt"),
    ("inventory.low-stock", "SKU-9 is running low")
];

var expected = events.Sum(e => services.Count(s => s.Patterns.Any(p => TopicMatcher.Matches(p, e.RoutingKey))));
var received = new Countdown(expected);
var channels = new List<IChannel>();

foreach (var (service, queue, patterns) in services)
{
    var channel = await connection.CreateChannelAsync();
    channels.Add(channel);

    await channel.QueueDeclareAsync(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);
    await channel.QueuePurgeAsync(queue);
    foreach (var pattern in patterns)
    {
        await channel.QueueBindAsync(queue: queue, exchange: ExchangeName, routingKey: pattern);
    }
    Log.Info(service, $"queue '{queue}' bound with {string.Join(", ", patterns)}");

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        Log.Info(service, $"{delivery.RoutingKey,-26} {Json.Text(delivery.Body)}");
        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        received.Signal();
    };
    await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);
}

foreach (var (routingKey, text) in events)
{
    var properties = new BasicProperties { Persistent = true, ContentType = "text/plain" };
    await publisher.BasicPublishAsync(ExchangeName, routingKey, mandatory: false, basicProperties: properties, body: Json.Bytes(text));
    Log.Info("publisher", $"published with key '{routingKey}'");
}

await received.WaitAsync();
Log.Info("demo", $"{expected} deliveries: one message can match several bindings, each queue gets one copy");

foreach (var channel in channels)
{
    await channel.CloseAsync();
    await channel.DisposeAsync();
}

static class TopicMatcher
{
    public static bool Matches(string pattern, string routingKey) =>
        Match(pattern.Split('.'), 0, routingKey.Split('.'), 0);

    private static bool Match(string[] pattern, int p, string[] words, int w)
    {
        if (p == pattern.Length)
        {
            return w == words.Length;
        }

        if (pattern[p] == "#")
        {
            for (var skip = w; skip <= words.Length; skip++)
            {
                if (Match(pattern, p + 1, words, skip))
                {
                    return true;
                }
            }
            return false;
        }

        return w < words.Length
            && (pattern[p] == "*" || pattern[p] == words[w])
            && Match(pattern, p + 1, words, w + 1);
    }
}
