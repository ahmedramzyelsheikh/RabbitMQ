using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string ExchangeName = "logs";

var mode = args.FirstOrDefault() ?? "demo";

await using var connection = await Broker.ConnectAsync($"03-pub-sub-{mode}");

switch (mode)
{
    case "publish":
    {
        await using var channel = await OpenChannelAsync(connection);
        await PublishAsync(channel, args.Length > 1 ? string.Join(' ', args.Skip(1)) : "info: hello subscribers");
        break;
    }
    case "subscribe":
    {
        var name = args.Length > 1 ? args[1] : $"subscriber-{Environment.ProcessId}";
        await using var channel = await OpenChannelAsync(connection);
        await SubscribeAsync(channel, name, () => { });
        await Shutdown.WaitForCtrlCAsync();
        break;
    }
    default:
        await RunDemoAsync(connection);
        break;
}

static async Task RunDemoAsync(IConnection connection)
{
    Log.Title("03 Publish/Subscribe: fanout exchange copies every message to every bound queue");

    await using var publisher = await OpenChannelAsync(connection);

    Log.Info("demo", "step 1: publish while nobody is subscribed -> the message is dropped (no queue bound)");
    await PublishAsync(publisher, "info: nobody will ever see this line");

    Log.Info("demo", "step 2: three independent subscribers, each with its own temporary queue");
    string[] subscribers = ["console-printer", "file-writer", "metrics"];
    string[] lines = ["info: app started", "warning: disk 85% full", "error: payment gateway timeout"];
    var received = new Countdown(subscribers.Length * lines.Length);

    var channels = new List<IChannel>();
    foreach (var name in subscribers)
    {
        var channel = await OpenChannelAsync(connection);
        channels.Add(channel);
        await SubscribeAsync(channel, name, received.Signal);
    }

    foreach (var line in lines)
    {
        await PublishAsync(publisher, line);
    }

    await received.WaitAsync();
    Log.Info("demo", $"every subscriber received all {lines.Length} lines ({subscribers.Length * lines.Length} deliveries in total)");

    foreach (var channel in channels)
    {
        await channel.CloseAsync();
        await channel.DisposeAsync();
    }
    Log.Info("demo", "subscriber channels closed -> exclusive auto-delete queues are gone");
}

static async Task<IChannel> OpenChannelAsync(IConnection connection)
{
    var channel = await connection.CreateChannelAsync();
    await channel.ExchangeDeclareAsync(exchange: ExchangeName, type: ExchangeType.Fanout, durable: true, autoDelete: false);
    return channel;
}

static async Task PublishAsync(IChannel channel, string line)
{
    await channel.BasicPublishAsync(exchange: ExchangeName, routingKey: string.Empty, body: Json.Bytes(line));
    Log.Info("publisher", $"published '{line}'");
}

static async Task SubscribeAsync(IChannel channel, string name, Action onReceived)
{
    var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true, arguments: null);
    await channel.QueueBindAsync(queue: queue.QueueName, exchange: ExchangeName, routingKey: string.Empty);
    Log.Info(name, $"bound server-named queue '{queue.QueueName}' to '{ExchangeName}'");

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += (_, delivery) =>
    {
        Log.Info(name, $"got '{Json.Text(delivery.Body)}'");
        onReceived();
        return Task.CompletedTask;
    };

    await channel.BasicConsumeAsync(queue: queue.QueueName, autoAck: true, consumer: consumer);
}
