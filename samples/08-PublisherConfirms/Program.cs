using System.Diagnostics;
using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

const string QueueName = "confirms.orders";
const string ExchangeName = "confirms.exchange";
const string AlternateExchange = "confirms.unrouted";
const string UnroutedQueue = "confirms.unrouted";
const int MessageCount = 2_000;

Log.Title("08 Publisher confirms, mandatory flag, returns and alternate exchanges");

await using var connection = await Broker.ConnectAsync("08-publisher-confirms");

var confirmOptions = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true);

await using (var setup = await connection.CreateChannelAsync())
{
    await setup.ExchangeDeclareAsync(AlternateExchange, ExchangeType.Fanout, durable: true, autoDelete: false);
    await setup.QueueDeclareAsync(UnroutedQueue, durable: true, exclusive: false, autoDelete: false, arguments: null);
    await setup.QueueBindAsync(UnroutedQueue, AlternateExchange, routingKey: string.Empty);

    await setup.ExchangeDeclareAsync(ExchangeName, ExchangeType.Direct, durable: true, autoDelete: false,
        arguments: new Dictionary<string, object?> { ["alternate-exchange"] = AlternateExchange });
    await setup.QueueDeclareAsync(QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
    await setup.QueueBindAsync(QueueName, ExchangeName, routingKey: "order.created");

    await setup.QueuePurgeAsync(QueueName);
    await setup.QueuePurgeAsync(UnroutedQueue);
}

await using var channel = await connection.CreateChannelAsync(confirmOptions);
channel.BasicReturnAsync += (_, returned) =>
{
    Log.Error("broker-return", $"returned: {returned.ReplyCode} {returned.ReplyText}, exchange '{returned.Exchange}', key '{returned.RoutingKey}'");
    return Task.CompletedTask;
};

var persistent = new BasicProperties { Persistent = true, ContentType = "text/plain" };

Log.Info("demo", $"strategy 1: await every publish one by one ({MessageCount} messages)");
var stopwatch = Stopwatch.StartNew();
for (var i = 0; i < MessageCount; i++)
{
    await channel.BasicPublishAsync(ExchangeName, "order.created", mandatory: true, basicProperties: persistent, body: Json.Bytes($"order {i}"));
}
Log.Info("one-by-one", $"{MessageCount} confirmed in {stopwatch.ElapsedMilliseconds} ms");

Log.Info("demo", $"strategy 2: publish in batches of 100 and await the batch ({MessageCount} messages)");
stopwatch.Restart();
var batch = new List<ValueTask>(100);
for (var i = 0; i < MessageCount; i++)
{
    batch.Add(channel.BasicPublishAsync(ExchangeName, "order.created", mandatory: true, basicProperties: persistent, body: Json.Bytes($"order {i}")));
    if (batch.Count == 100 || i == MessageCount - 1)
    {
        foreach (var pending in batch)
        {
            await pending;
        }
        batch.Clear();
    }
}
Log.Info("batched", $"{MessageCount} confirmed in {stopwatch.ElapsedMilliseconds} ms");

Log.Info("demo", "case 3: mandatory publish to the default exchange with a key no queue has -> PublishException(IsReturn)");
try
{
    await channel.BasicPublishAsync(string.Empty, "no.such.queue", mandatory: true, basicProperties: persistent, body: Json.Bytes("lost?"));
}
catch (PublishException ex)
{
    Log.Error("publisher", $"PublishException: IsReturn={ex.IsReturn}, sequence={ex.PublishSequenceNumber}");
}

Log.Info("demo", "case 4: unroutable key on an exchange that has an alternate exchange -> message is kept, not returned");
await channel.BasicPublishAsync(ExchangeName, "order.unknown-event", mandatory: true, basicProperties: persistent, body: Json.Bytes("rescued by AE"));
var rescued = await channel.BasicGetAsync(UnroutedQueue, autoAck: true);
Log.Info("alternate-exch", rescued is null ? "nothing in AE queue" : $"found '{Json.Text(rescued.Body)}' in '{UnroutedQueue}'");

Log.Info("demo", "case 5: publish to an exchange that does not exist -> broker closes the channel (404 NOT_FOUND)");
await using (var doomed = await connection.CreateChannelAsync(confirmOptions))
{
    try
    {
        await doomed.BasicPublishAsync("exchange.that.does.not.exist", "x", mandatory: false, basicProperties: persistent, body: Json.Bytes("x"));
        await doomed.QueueDeclarePassiveAsync(QueueName);
    }
    catch (OperationInterruptedException ex)
    {
        Log.Error("publisher", $"{ex.GetType().Name}: {ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText}");
    }
    Log.Info("publisher", $"channel is open: {doomed.IsOpen} -> open a new channel after a channel-level error");
}

var depth = await channel.MessageCountAsync(QueueName);
Log.Info("demo", $"queue '{QueueName}' now holds {depth} persistent messages");
await channel.QueuePurgeAsync(QueueName);
