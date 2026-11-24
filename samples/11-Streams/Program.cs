using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string StreamName = "events.stream";

Log.Title("11 Streams: an append-only log, consumers read from any offset, nothing is removed on ack");

await using var connection = await Broker.ConnectAsync("11-streams");

await using (var setup = await connection.CreateChannelAsync())
{
    await setup.QueueDeleteAsync(StreamName);
    await setup.QueueDeclareAsync(StreamName, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
    {
        ["x-queue-type"] = "stream",
        ["x-max-age"] = "7D",
        ["x-stream-max-segment-size-bytes"] = 10_000_000
    });
}

await using var publisher = await connection.CreateChannelAsync(
    new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));

for (var i = 0; i < 10; i++)
{
    await PublishAsync(publisher, $"price-tick #{i}");
}
Log.Info("publisher", "10 events appended to the stream (offsets 0..9)");

var replayAll = new Countdown(12);
var fromFive = new Countdown(7);
var onlyNew = new Countdown(2);

await using var reporting = await StartReaderAsync(connection, "reporting", "first", replayAll);
await using var analytics = await StartReaderAsync(connection, "analytics", 5L, fromFive);
await using var live = await StartReaderAsync(connection, "live-dashboard", "next", onlyNew);

await Task.Delay(1_000);
Log.Info("publisher", "appending 2 more events");
await PublishAsync(publisher, "price-tick #10");
await PublishAsync(publisher, "price-tick #11");

await Task.WhenAll(replayAll.WaitAsync(), fromFive.WaitAsync(), onlyNew.WaitAsync());
Log.Info("demo", "reporting replayed all 12, analytics started at offset 5 (7 events), live saw only the 2 new ones");
var lateJoiner = new Countdown(12);
await using (await StartReaderAsync(connection, "late-joiner", "first", lateJoiner))
{
    await lateJoiner.WaitAsync();
}
Log.Info("demo", "a consumer that joins later still reads all 12 events: acks never delete from a stream");

static async Task PublishAsync(IChannel channel, string text) =>
    await channel.BasicPublishAsync(string.Empty, StreamName, mandatory: false,
        basicProperties: new BasicProperties { Persistent = true }, body: Json.Bytes(text));

static async Task<IChannel> StartReaderAsync(IConnection connection, string name, object offset, Countdown countdown)
{
    var channel = await connection.CreateChannelAsync();
    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 50, global: false);

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        var position = delivery.BasicProperties.Headers?["x-stream-offset"];
        Log.Info(name, $"offset {position,-3} {Json.Text(delivery.Body)}");
        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        countdown.Signal();
    };

    await channel.BasicConsumeAsync(StreamName, autoAck: false, consumerTag: string.Empty, noLocal: false, exclusive: false,
        arguments: new Dictionary<string, object?> { ["x-stream-offset"] = offset }, consumer: consumer);
    Log.Info(name, $"reading from offset '{offset}'");
    return channel;
}
