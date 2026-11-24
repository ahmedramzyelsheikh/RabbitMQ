using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

Log.Title("10 Queue features: TTL, expiration, max-length, priority, x-expires, quorum delivery-limit, single active consumer");

await using var connection = await Broker.ConnectAsync("10-queue-features");

await QueueTtlAsync(connection);
await PerMessageExpirationAsync(connection);
await MaxLengthDropHeadAsync(connection);
await MaxLengthRejectPublishAsync(connection);
await PriorityQueueAsync(connection);
await QueueExpiresAsync(connection);
await QuorumDeliveryLimitAsync(connection);
await SingleActiveConsumerAsync(connection);

Log.Info("demo", "all queue features demonstrated");

static async Task<IChannel> FreshQueueAsync(IConnection connection, string queue, Dictionary<string, object?> arguments, CreateChannelOptions? options = null)
{
    var channel = await connection.CreateChannelAsync(options);
    await channel.QueueDeleteAsync(queue);
    await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments: arguments);
    return channel;
}

static async Task PublishAsync(IChannel channel, string queue, string text, BasicProperties? properties = null) =>
    await channel.BasicPublishAsync(string.Empty, queue, mandatory: false, basicProperties: properties ?? new BasicProperties(), body: Json.Bytes(text));

static async Task<List<string>> DrainAsync(IChannel channel, string queue)
{
    var bodies = new List<string>();
    while (await channel.BasicGetAsync(queue, autoAck: true) is { } result)
    {
        bodies.Add(Json.Text(result.Body));
    }
    return bodies;
}

static async Task QueueTtlAsync(IConnection connection)
{
    const string queue = "features.ttl";
    Log.Info("ttl", "x-message-ttl = 1000 ms on the queue: every message expires after 1s");
    await using var channel = await FreshQueueAsync(connection, queue, new() { ["x-message-ttl"] = 1_000 });
    await PublishAsync(channel, queue, "short-lived");
    Log.Info("ttl", $"right away: {await channel.MessageCountAsync(queue)} message(s)");
    await Task.Delay(1_500);
    Log.Info("ttl", $"after 1.5s: {await channel.MessageCountAsync(queue)} message(s)");
}

static async Task PerMessageExpirationAsync(IConnection connection)
{
    const string queue = "features.expiration";
    Log.Info("expiration", "per-message 'expiration' property (string, ms) on two messages in the same queue");
    await using var channel = await FreshQueueAsync(connection, queue, new());
    await PublishAsync(channel, queue, "expires in 300 ms", new BasicProperties { Expiration = "300" });
    await PublishAsync(channel, queue, "never expires");
    await Task.Delay(600);
    Log.Info("expiration", $"left after 600 ms: {string.Join(", ", await DrainAsync(channel, queue))}");
}

static async Task MaxLengthDropHeadAsync(IConnection connection)
{
    const string queue = "features.max-length.drop-head";
    Log.Info("max-length", "x-max-length = 3, x-overflow = drop-head: the oldest messages are dropped");
    await using var channel = await FreshQueueAsync(connection, queue, new() { ["x-max-length"] = 3, ["x-overflow"] = "drop-head" });
    for (var i = 1; i <= 5; i++)
    {
        await PublishAsync(channel, queue, $"reading {i}");
    }
    Log.Info("max-length", $"kept: {string.Join(", ", await DrainAsync(channel, queue))}");
}

static async Task MaxLengthRejectPublishAsync(IConnection connection)
{
    const string queue = "features.max-length.reject";
    Log.Info("max-length", "x-max-length = 2, x-overflow = reject-publish: with confirms the publisher gets a nack");
    var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
    await using var channel = await FreshQueueAsync(connection, queue, new() { ["x-max-length"] = 2, ["x-overflow"] = "reject-publish" }, options);
    for (var i = 1; i <= 3; i++)
    {
        try
        {
            await PublishAsync(channel, queue, $"job {i}");
            Log.Info("max-length", $"job {i} confirmed (ack)");
        }
        catch (PublishException ex)
        {
            Log.Error("max-length", $"job {i} rejected by the broker (nack, IsReturn={ex.IsReturn}) -> apply back-pressure");
        }
    }
    await channel.QueueDeleteAsync(queue);
}

static async Task PriorityQueueAsync(IConnection connection)
{
    const string queue = "features.priority";
    Log.Info("priority", "x-max-priority = 10: higher 'priority' property is delivered first");
    await using var channel = await FreshQueueAsync(connection, queue, new() { ["x-max-priority"] = 10 });
    await PublishAsync(channel, queue, "newsletter (1)", new BasicProperties { Priority = 1 });
    await PublishAsync(channel, queue, "password reset (9)", new BasicProperties { Priority = 9 });
    await PublishAsync(channel, queue, "order receipt (5)", new BasicProperties { Priority = 5 });
    await PublishAsync(channel, queue, "no priority = 0");
    Log.Info("priority", $"delivery order: {string.Join(" > ", await DrainAsync(channel, queue))}");
}

static async Task QueueExpiresAsync(IConnection connection)
{
    const string queue = "features.expires";
    Log.Info("x-expires", "x-expires = 1000 ms: an unused queue deletes itself");
    await using (await FreshQueueAsync(connection, queue, new() { ["x-expires"] = 1_000 }))
    {
    }

    await Task.Delay(2_000);
    await using var probe = await connection.CreateChannelAsync();
    try
    {
        await probe.QueueDeclarePassiveAsync(queue);
        Log.Info("x-expires", "queue still exists");
    }
    catch (OperationInterruptedException ex)
    {
        Log.Info("x-expires", $"passive declare failed: {ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText}");
    }
}

static async Task QuorumDeliveryLimitAsync(IConnection connection)
{
    const string queue = "features.quorum";
    const string deadLetterExchange = "features.quorum.dlx";
    const string deadLetterQueue = "features.quorum.dlq";
    Log.Info("quorum", "quorum queue with x-delivery-limit = 2: a poison message stops looping and is dead-lettered");

    await using var channel = await connection.CreateChannelAsync();
    await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Fanout, durable: true, autoDelete: false);
    await channel.QueueDeleteAsync(deadLetterQueue);
    await channel.QueueDeclareAsync(deadLetterQueue, durable: true, exclusive: false, autoDelete: false, arguments: null);
    await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, string.Empty);
    await channel.QueueDeleteAsync(queue);
    await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
    {
        ["x-queue-type"] = "quorum",
        ["x-delivery-limit"] = 2,
        ["x-dead-letter-exchange"] = deadLetterExchange
    });

    var deliveries = 0;
    var deadLettered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    await using var consumerChannel = await connection.CreateChannelAsync();
    await consumerChannel.BasicQosAsync(0, 1, false);
    var consumer = new AsyncEventingBasicConsumer(consumerChannel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        deliveries++;
        var count = delivery.BasicProperties.Headers?.TryGetValue("x-delivery-count", out var c) == true ? c : 0;
        Log.Error("quorum", $"delivery #{deliveries} (x-delivery-count={count}, redelivered={delivery.Redelivered}) -> nack requeue=true");
        await consumerChannel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true);
    };
    await consumerChannel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);

    await PublishAsync(channel, queue, "poison message");

    for (var i = 0; i < 50 && !deadLettered.Task.IsCompleted; i++)
    {
        if (await channel.MessageCountAsync(deadLetterQueue) > 0)
        {
            deadLettered.TrySetResult();
        }
        await Task.Delay(100);
    }

    Log.Info("quorum", $"dead-lettered after {deliveries} deliveries: {await channel.MessageCountAsync(deadLetterQueue)} message in '{deadLetterQueue}'");
}

static async Task SingleActiveConsumerAsync(IConnection connection)
{
    const string queue = "features.single-active";
    Log.Info("sac", "x-single-active-consumer = true: only one consumer gets messages, the other is a hot standby");

    await using var setup = await FreshQueueAsync(connection, queue, new() { ["x-single-active-consumer"] = true });
    var received = new Countdown(6);

    async Task<IChannel> StartAsync(string name)
    {
        var channel = await connection.CreateChannelAsync();
        await channel.BasicQosAsync(0, 1, false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            Log.Info(name, $"processed {Json.Text(delivery.Body)}");
            await channel.BasicAckAsync(delivery.DeliveryTag, false);
            received.Signal();
        };
        await channel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);
        return channel;
    }

    var primary = await StartAsync("sac-primary");
    await using var standby = await StartAsync("sac-standby");

    for (var i = 1; i <= 3; i++)
    {
        await PublishAsync(setup, queue, $"event {i}");
    }
    while (received.Remaining > 3)
    {
        await Task.Delay(50);
    }

    Log.Info("sac", "primary goes away -> standby becomes active, order is preserved");
    await primary.CloseAsync();
    await primary.DisposeAsync();

    for (var i = 4; i <= 6; i++)
    {
        await PublishAsync(setup, queue, $"event {i}");
    }
    await received.WaitAsync();
}
