using System.Collections.Concurrent;
using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string WorkExchange = "payments";
const string WorkQueue = "payments.process";
const string RetryExchange = "payments.retry";
const string RetryQueue = "payments.retry.2s";
const string ParkingExchange = "payments.parking-lot";
const string ParkingQueue = "payments.parking-lot";
const string RoutingKey = "payment";
const int MaxAttempts = 3;

Log.Title("09 Dead letter exchange + delayed retry (TTL queue) + parking lot");

await using var connection = await Broker.ConnectAsync("09-dead-letter-retry");

await using (var setup = await connection.CreateChannelAsync())
{
    await setup.ExchangeDeclareAsync(WorkExchange, ExchangeType.Direct, durable: true, autoDelete: false);
    await setup.ExchangeDeclareAsync(RetryExchange, ExchangeType.Direct, durable: true, autoDelete: false);
    await setup.ExchangeDeclareAsync(ParkingExchange, ExchangeType.Fanout, durable: true, autoDelete: false);

    await setup.QueueDeclareAsync(WorkQueue, durable: true, exclusive: false, autoDelete: false,
        arguments: new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-dead-letter-exchange"] = RetryExchange
        });
    await setup.QueueBindAsync(WorkQueue, WorkExchange, RoutingKey);

    await setup.QueueDeclareAsync(RetryQueue, durable: true, exclusive: false, autoDelete: false,
        arguments: new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-message-ttl"] = 2_000,
            ["x-dead-letter-exchange"] = WorkExchange
        });
    await setup.QueueBindAsync(RetryQueue, RetryExchange, RoutingKey);

    await setup.QueueDeclareAsync(ParkingQueue, durable: true, exclusive: false, autoDelete: false,
        arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
    await setup.QueueBindAsync(ParkingQueue, ParkingExchange, string.Empty);

    foreach (var queue in new[] { WorkQueue, RetryQueue, ParkingQueue })
    {
        await setup.QueuePurgeAsync(queue);
    }
}

var outcomes = new Countdown(3);
var attemptsSeen = new ConcurrentDictionary<string, int>();

await using var worker = await connection.CreateChannelAsync();
await worker.BasicQosAsync(0, 5, false);
var consumer = new AsyncEventingBasicConsumer(worker);
consumer.ReceivedAsync += async (_, delivery) =>
{
    var payment = Json.Text(delivery.Body);
    var paymentId = delivery.BasicProperties.MessageId ?? "?";
    var failedBefore = MessageHeaders.DeathCount(delivery.BasicProperties, WorkQueue, "rejected");
    var attempt = failedBefore + 1;
    attemptsSeen.AddOrUpdate(paymentId, 1, (_, n) => n + 1);

    try
    {
        PaymentGateway.Charge(paymentId, (int)attempt);
        await worker.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        Log.Info("worker", $"{payment}: attempt {attempt} succeeded -> ack");
        outcomes.Signal();
    }
    catch (Exception ex) when (attempt < MaxAttempts)
    {
        Log.Error("worker", $"{payment}: attempt {attempt} failed ({ex.Message}) -> nack, retry in 2s");
        await worker.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false);
    }
    catch (Exception ex)
    {
        var headers = new Dictionary<string, object?>
        {
            ["x-error"] = ex.Message,
            ["x-attempts"] = attempt,
            ["x-original-queue"] = WorkQueue
        };
        var parked = new BasicProperties { Persistent = true, MessageId = paymentId, Headers = headers };
        await worker.BasicPublishAsync(ParkingExchange, string.Empty, mandatory: false, basicProperties: parked, body: delivery.Body);
        await worker.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        Log.Error("worker", $"{payment}: attempt {attempt} failed ({ex.Message}) -> moved to parking lot");
        outcomes.Signal();
    }
};
await worker.BasicConsumeAsync(WorkQueue, autoAck: false, consumer: consumer);

await using var publisher = await connection.CreateChannelAsync();
foreach (var (id, text) in new[] { ("pay-1", "pay-1 100 EGP (healthy)"), ("pay-2", "pay-2 250 EGP (gateway flaky)"), ("pay-3", "pay-3 999 EGP (card is stolen)") })
{
    var properties = new BasicProperties { Persistent = true, MessageId = id };
    await publisher.BasicPublishAsync(WorkExchange, RoutingKey, mandatory: false, basicProperties: properties, body: Json.Bytes(text));
    Log.Info("publisher", $"published {text}");
}

await outcomes.WaitAsync(TimeSpan.FromSeconds(30));

var parkedMessage = await publisher.BasicGetAsync(ParkingQueue, autoAck: true);
if (parkedMessage is not null)
{
    Log.Info("parking-lot", $"'{Json.Text(parkedMessage.Body)}' headers: {MessageHeaders.Describe(parkedMessage.BasicProperties)}");
}

Log.Info("demo", "x-death count tells the consumer how many times the message was rejected from the work queue");

static class PaymentGateway
{
    public static void Charge(string paymentId, int attempt)
    {
        switch (paymentId)
        {
            case "pay-2" when attempt < 3:
                throw new TimeoutException("gateway timeout");
            case "pay-3":
                throw new InvalidOperationException("card rejected");
        }
    }
}
