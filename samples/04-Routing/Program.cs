using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string ExchangeName = "logs.direct";

var mode = args.FirstOrDefault() ?? "demo";

await using var connection = await Broker.ConnectAsync($"04-routing-{mode}");

switch (mode)
{
    case "publish":
    {
        var severity = args.Length > 1 ? args[1] : "info";
        var text = args.Length > 2 ? string.Join(' ', args.Skip(2)) : "hello";
        await using var channel = await OpenChannelAsync(connection);
        await PublishAsync(channel, severity, text);
        break;
    }
    case "subscribe":
    {
        var severities = args.Skip(1).DefaultIfEmpty("error").ToArray();
        await using var channel = await OpenChannelAsync(connection);
        await SubscribeAsync(channel, $"sub[{string.Join(',', severities)}]", severities, () => { });
        await Shutdown.WaitForCtrlCAsync();
        break;
    }
    default:
        await RunDemoAsync(connection);
        break;
}

static async Task RunDemoAsync(IConnection connection)
{
    Log.Title("04 Routing: direct exchange delivers by exact routing key match");

    await using var publisher = await OpenChannelAsync(connection);
    await using var diskChannel = await OpenChannelAsync(connection);
    await using var screenChannel = await OpenChannelAsync(connection);

    (string Severity, string Text)[] logs =
    [
        ("info", "user 42 logged in"),
        ("warning", "slow query 2.3s"),
        ("error", "cannot reach SMTP server"),
        ("info", "order 1001 created"),
        ("error", "invoice 77 failed to render"),
        ("debug", "nobody binds 'debug' so this is dropped")
    ];

    var expected = logs.Count(l => l.Severity == "error") + logs.Count(l => l.Severity is "info" or "warning" or "error");
    var received = new Countdown(expected);

    await SubscribeAsync(diskChannel, "errors-to-disk", ["error"], received.Signal);
    await SubscribeAsync(screenChannel, "screen", ["info", "warning", "error"], received.Signal);

    foreach (var (severity, text) in logs)
    {
        await PublishAsync(publisher, severity, text);
    }

    await received.WaitAsync();
    Log.Info("demo", "'error' went to both queues, info/warning only to 'screen', 'debug' matched no binding");
}

static async Task<IChannel> OpenChannelAsync(IConnection connection)
{
    var channel = await connection.CreateChannelAsync();
    await channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Direct, durable: true, autoDelete: false);
    return channel;
}

static async Task PublishAsync(IChannel channel, string severity, string text)
{
    await channel.BasicPublishAsync(exchange: ExchangeName, routingKey: severity, body: Json.Bytes(text));
    Log.Info("publisher", $"[{severity}] {text}");
}

static async Task SubscribeAsync(IChannel channel, string name, string[] severities, Action onReceived)
{
    var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true, arguments: null);
    foreach (var severity in severities)
    {
        await channel.QueueBindAsync(queue: queue.QueueName, exchange: ExchangeName, routingKey: severity);
    }
    Log.Info(name, $"bound to keys: {string.Join(", ", severities)}");

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += (_, delivery) =>
    {
        Log.Info(name, $"[{delivery.RoutingKey}] {Json.Text(delivery.Body)}");
        onReceived();
        return Task.CompletedTask;
    };

    await channel.BasicConsumeAsync(queue: queue.QueueName, autoAck: true, consumer: consumer);
}
