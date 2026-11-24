using System.Text;
using Learning.Shared;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace OutboxInbox;

public sealed class SimulatedCrashException(string message) : Exception(message);

public sealed class OutboxDispatcher(IChannel confirmChannel, Func<ShopDbContext> dbFactory)
{
    public async Task<int> DispatchPendingAsync(bool crashBeforeMarkingSent = false)
    {
        await using var db = dbFactory();

        var pending = await db.OutboxMessages
            .Where(m => m.SentUtc == null)
            .OrderBy(m => m.OccurredUtc)
            .Take(100)
            .ToListAsync();

        foreach (var message in pending)
        {
            var properties = new BasicProperties
            {
                MessageId = message.Id.ToString(),
                Type = message.Type,
                ContentType = Json.ContentType,
                Persistent = true,
                Timestamp = new AmqpTimestamp(new DateTimeOffset(message.OccurredUtc, TimeSpan.Zero).ToUnixTimeSeconds())
            };

            try
            {
                await confirmChannel.BasicPublishAsync(message.Exchange, message.RoutingKey, mandatory: true,
                    basicProperties: properties, body: Encoding.UTF8.GetBytes(message.Payload));
                message.SentUtc = DateTime.UtcNow;
                Log.Info("outbox", $"published {message.Type} {message.Id.ToString()[^8..]} and the broker confirmed it");
            }
            catch (PublishException ex)
            {
                message.Attempts++;
                message.LastError = ex.IsReturn ? "unroutable" : "nacked by broker";
                Log.Error("outbox", $"publish of {message.Id.ToString()[^8..]} failed: {message.LastError}, will retry");
            }
        }

        if (crashBeforeMarkingSent)
        {
            throw new SimulatedCrashException($"process died after publishing {pending.Count} message(s) but before saving SentUtc");
        }

        await db.SaveChangesAsync();
        return pending.Count(m => m.SentUtc is not null);
    }
}
