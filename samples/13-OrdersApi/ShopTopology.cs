using RabbitMQ.Client;

namespace OrdersApi;

public sealed record OrderCreated(Guid OrderId, string CustomerEmail, decimal Total, DateTime CreatedUtc);

public static class ShopTopology
{
    public const string Exchange = "shop.events";
    public const string DeadLetterExchange = "shop.dead-letter";
    public const string NotificationsQueue = "shop.notifications";
    public const string DeadLetterQueue = "shop.dead-letter";
    public const string OrderCreatedKey = "order.created";

    public static async Task DeclareAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true, autoDelete: false);
        await channel.ExchangeDeclareAsync(DeadLetterExchange, ExchangeType.Fanout, durable: true, autoDelete: false);

        await channel.QueueDeclareAsync(DeadLetterQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
        await channel.QueueBindAsync(DeadLetterQueue, DeadLetterExchange, string.Empty);

        await channel.QueueDeclareAsync(NotificationsQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-dead-letter-exchange"] = DeadLetterExchange,
                ["x-delivery-limit"] = 5
            });
        await channel.QueueBindAsync(NotificationsQueue, Exchange, "order.#");
    }
}
