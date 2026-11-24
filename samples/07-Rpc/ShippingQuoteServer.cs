using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Rpc;

public sealed class ShippingQuoteServer : IAsyncDisposable
{
    public const string QueueName = "rpc.shipping-quote";

    private static readonly Dictionary<string, decimal> PricePerKg = new()
    {
        ["EG"] = 25m,
        ["SA"] = 90m,
        ["AE"] = 85m
    };

    private readonly IChannel channel;

    private ShippingQuoteServer(IChannel channel) => this.channel = channel;

    public static async Task<ShippingQuoteServer> StartAsync(IConnection connection)
    {
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: 4);
        var channel = await connection.CreateChannelAsync(options);
        await channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
        await channel.QueuePurgeAsync(QueueName);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false);

        var server = new ShippingQuoteServer(channel);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += server.OnRequestAsync;
        await channel.BasicConsumeAsync(queue: QueueName, autoAck: false, consumer: consumer);

        Log.Info("server", $"serving requests on '{QueueName}'");
        return server;
    }

    private async Task OnRequestAsync(object sender, BasicDeliverEventArgs delivery)
    {
        var request = Json.Deserialize<QuoteRequest>(delivery.Body);
        var replyTo = delivery.BasicProperties.ReplyTo;
        var correlationId = delivery.BasicProperties.CorrelationId;

        await Task.Delay(Random.Shared.Next(50, 300));

        var price = PricePerKg.GetValueOrDefault(request.Country, 150m) * request.WeightKg;
        var reply = new QuoteReply(request.Country, request.WeightKg, price, Environment.MachineName);
        Log.Info("server", $"request {correlationId?[..8]} {request.Country} {request.WeightKg}kg -> {price}");

        if (!string.IsNullOrEmpty(replyTo))
        {
            var properties = new BasicProperties
            {
                CorrelationId = correlationId,
                ContentType = Json.ContentType
            };
            await channel.BasicPublishAsync(exchange: string.Empty, routingKey: replyTo, mandatory: false,
                basicProperties: properties, body: Json.Serialize(reply));
        }

        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
    }

    public async ValueTask DisposeAsync()
    {
        await channel.CloseAsync();
        await channel.DisposeAsync();
    }
}
