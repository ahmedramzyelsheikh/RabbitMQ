using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string ExchangeName = "documents.headers";

Log.Title("06 Headers exchange: route on message headers instead of the routing key");

await using var connection = await Broker.ConnectAsync("06-headers");
await using var publisher = await connection.CreateChannelAsync();
await publisher.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Headers, durable: true, autoDelete: false);

(string Name, string Queue, Dictionary<string, object?> Binding)[] subscribers =
[
    ("pdf-reports", "documents.pdf-reports", new()
    {
        ["x-match"] = "all",
        ["format"] = "pdf",
        ["type"] = "report"
    }),
    ("pdf-or-urgent", "documents.pdf-or-urgent", new()
    {
        ["x-match"] = "any",
        ["format"] = "pdf",
        ["priority"] = "urgent"
    })
];

(string Title, Dictionary<string, object?> Headers)[] documents =
[
    ("Q3 sales report", new() { ["format"] = "pdf", ["type"] = "report" }),
    ("Invoice 7781", new() { ["format"] = "pdf", ["type"] = "invoice" }),
    ("Contract draft", new() { ["format"] = "docx", ["priority"] = "urgent" }),
    ("Team notes", new() { ["format"] = "docx", ["type"] = "report" })
];

var received = new Countdown(4);
var channels = new List<IChannel>();

foreach (var (name, queue, binding) in subscribers)
{
    var channel = await connection.CreateChannelAsync();
    channels.Add(channel);
    await channel.QueueDeclareAsync(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);
    await channel.QueuePurgeAsync(queue);
    await channel.QueueBindAsync(queue: queue, exchange: ExchangeName, routingKey: string.Empty, arguments: binding);
    Log.Info(name, $"bound with {string.Join(", ", binding.Select(b => $"{b.Key}={b.Value}"))}");

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        Log.Info(name, $"'{Json.Text(delivery.Body)}' headers: {MessageHeaders.Describe(delivery.BasicProperties)}");
        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        received.Signal();
    };
    await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);
}

foreach (var (title, headers) in documents)
{
    var properties = new BasicProperties { Headers = headers, Persistent = true };
    await publisher.BasicPublishAsync(ExchangeName, routingKey: "ignored-by-headers-exchange", mandatory: false,
        basicProperties: properties, body: Json.Bytes(title));
    Log.Info("publisher", $"'{title}' with {string.Join(", ", headers.Select(h => $"{h.Key}={h.Value}"))}");
}

await received.WaitAsync();
Log.Info("demo", "x-match=all needs every header to match, x-match=any needs one; 'Team notes' matched nothing");

foreach (var channel in channels)
{
    await channel.CloseAsync();
    await channel.DisposeAsync();
}
