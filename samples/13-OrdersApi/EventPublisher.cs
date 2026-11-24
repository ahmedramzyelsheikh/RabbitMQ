using Learning.Shared;
using RabbitMQ.Client;

namespace OrdersApi;

public sealed class EventPublisher(IConnection connection, ILogger<EventPublisher> logger) : IAsyncDisposable
{
    private readonly SemaphoreSlim gate = new(1, 1);
    private IChannel? channel;

    public async Task PublishAsync<TEvent>(string routingKey, TEvent @event, CancellationToken cancellationToken)
    {
        var properties = new BasicProperties
        {
            MessageId = Guid.NewGuid().ToString(),
            Type = typeof(TEvent).Name,
            ContentType = Json.ContentType,
            Persistent = true,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            AppId = "orders-api"
        };

        await gate.WaitAsync(cancellationToken);
        try
        {
            if (channel is not { IsOpen: true })
            {
                if (channel is not null)
                {
                    await channel.DisposeAsync();
                }

                channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                    cancellationToken);
            }

            await channel.BasicPublishAsync(ShopTopology.Exchange, routingKey, mandatory: true,
                basicProperties: properties, body: Json.Serialize(@event), cancellationToken: cancellationToken);
        }
        finally
        {
            gate.Release();
        }

        logger.LogInformation("Published {EventType} {MessageId} with key {RoutingKey} (confirmed)", properties.Type, properties.MessageId, routingKey);
    }

    public async ValueTask DisposeAsync()
    {
        if (channel is not null)
        {
            await channel.DisposeAsync();
        }
        gate.Dispose();
    }
}
