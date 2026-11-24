using Learning.Shared;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OutboxInbox;

public sealed class EmailConsumer(IChannel channel, Func<ShopDbContext> dbFactory, Action<bool> onHandled)
{
    private const string Name = "email-service";

    public async Task StartAsync()
    {
        await channel.BasicQosAsync(0, 10, false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += HandleAsync;
        await channel.BasicConsumeAsync(Topology.EmailQueue, autoAck: false, consumer: consumer);
    }

    private async Task HandleAsync(object sender, BasicDeliverEventArgs delivery)
    {
        var messageId = delivery.BasicProperties.MessageId;
        if (string.IsNullOrEmpty(messageId))
        {
            Log.Error(Name, "message without MessageId cannot be de-duplicated -> reject");
            await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false);
            return;
        }

        await using var db = dbFactory();

        if (await db.InboxMessages.AnyAsync(m => m.MessageId == messageId && m.Consumer == Name))
        {
            Log.Error(Name, $"duplicate {messageId[^8..]} already in inbox -> ack and skip");
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
            onHandled(false);
            return;
        }

        var orderPlaced = Json.Deserialize<OrderPlaced>(delivery.Body);

        db.SentEmails.Add(new SentEmail
        {
            Id = Guid.CreateVersion7(),
            OrderId = orderPlaced.OrderId,
            To = orderPlaced.CustomerEmail,
            SentUtc = DateTime.UtcNow
        });
        db.InboxMessages.Add(new InboxMessage
        {
            MessageId = messageId,
            Consumer = Name,
            ProcessedUtc = DateTime.UtcNow
        });

        try
        {
            await db.SaveChangesAsync();
            Log.Info(Name, $"email to {orderPlaced.CustomerEmail} for order {orderPlaced.OrderId.ToString()[^8..]} (inbox {messageId[^8..]})");
            onHandled(true);
        }
        catch (DbUpdateException)
        {
            Log.Error(Name, $"{messageId[^8..]} was processed concurrently by another instance -> skip");
            onHandled(false);
        }

        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
    }
}
