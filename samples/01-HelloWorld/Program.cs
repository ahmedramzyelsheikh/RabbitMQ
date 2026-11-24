using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string QueueName = "hello";

var mode = args.FirstOrDefault() ?? "demo";

await using var connection = await Broker.ConnectAsync($"01-hello-world-{mode}");

switch (mode)
{
    case "send":
    {
        await using var channel = await OpenChannelAsync(connection);
        var text = args.Length > 1 ? string.Join(' ', args.Skip(1)) : "Hello World!";
        await SendAsync(channel, text);
        break;
    }
    case "receive":
    {
        await using var channel = await OpenChannelAsync(connection);
        await StartConsumerAsync(channel, () => { });
        await Shutdown.WaitForCtrlCAsync();
        break;
    }
    default:
    {
        Log.Title("01 Hello World: producer -> default exchange -> queue 'hello' -> consumer");

        await using var consumerChannel = await OpenChannelAsync(connection);
        await using var producerChannel = await OpenChannelAsync(connection);
        await producerChannel.QueuePurgeAsync(QueueName);

        string[] texts = ["Hello World!", "Second message", "Third message"];
        var received = new Countdown(texts.Length);

        await StartConsumerAsync(consumerChannel, received.Signal);

        foreach (var text in texts)
        {
            await SendAsync(producerChannel, text);
        }

        await received.WaitAsync();
        Log.Info("demo", "done: every message was delivered exactly to the queue named by the routing key");
        break;
    }
}

static async Task<IChannel> OpenChannelAsync(IConnection connection)
{
    var channel = await connection.CreateChannelAsync();
    await channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
    return channel;
}

static async Task SendAsync(IChannel channel, string text)
{
    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: QueueName, body: Json.Bytes(text));
    Log.Info("producer", $"sent '{text}'");
}

static async Task StartConsumerAsync(IChannel channel, Action onReceived)
{
    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += (_, delivery) =>
    {
        Log.Info("consumer", $"received '{Json.Text(delivery.Body)}' (delivery tag {delivery.DeliveryTag})");
        onReceived();
        return Task.CompletedTask;
    };

    await channel.BasicConsumeAsync(queue: QueueName, autoAck: true, consumer: consumer);
    Log.Info("consumer", $"waiting for messages on '{QueueName}'");
}
