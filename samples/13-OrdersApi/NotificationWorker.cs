using System.Text.Json;
using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrdersApi;

public sealed class NotificationWorker(IConnection connection, ILogger<NotificationWorker> logger) : BackgroundService
{
    private IChannel? channel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 20, global: false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += HandleAsync;

        await channel.BasicConsumeAsync(ShopTopology.NotificationsQueue, autoAck: false, consumer: consumer, stoppingToken);
        logger.LogInformation("Consuming {Queue}", ShopTopology.NotificationsQueue);
    }

    private async Task HandleAsync(object sender, BasicDeliverEventArgs delivery)
    {
        var target = channel!;
        try
        {
            var orderCreated = Json.Deserialize<OrderCreated>(delivery.Body);
            await Task.Delay(200);
            logger.LogInformation("Email sent to {Email} for order {OrderId} ({Total} EGP)",
                orderCreated.CustomerEmail, orderCreated.OrderId, orderCreated.Total);
            await target.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Malformed message {MessageId} -> dead letter", delivery.BasicProperties.MessageId);
            await target.BasicRejectAsync(delivery.DeliveryTag, requeue: false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Temporary failure for {MessageId} -> requeue (quorum delivery-limit stops loops)", delivery.BasicProperties.MessageId);
            await target.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (channel is not null)
        {
            await channel.CloseAsync(cancellationToken);
            await channel.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }
}
