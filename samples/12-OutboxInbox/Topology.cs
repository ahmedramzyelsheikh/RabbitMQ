using RabbitMQ.Client;

namespace OutboxInbox;

public static class Topology
{
    public const string Exchange = "shop.outbox-demo";
    public const string EmailQueue = "email.order-placed";
    public const string OrderPlacedKey = "order.placed";

    public static async Task DeclareAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true, autoDelete: false);
        await channel.QueueDeclareAsync(EmailQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
        await channel.QueueBindAsync(EmailQueue, Exchange, OrderPlacedKey);
        await channel.QueuePurgeAsync(EmailQueue);
    }
}
