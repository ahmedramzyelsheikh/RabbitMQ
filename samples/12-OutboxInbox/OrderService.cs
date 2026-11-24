using System.Text.Json;
using Learning.Shared;

namespace OutboxInbox;

public sealed class OrderService(Func<ShopDbContext> dbFactory)
{
    public async Task<Guid> PlaceOrderAsync(string customerEmail, decimal total)
    {
        await using var db = dbFactory();

        var now = DateTime.UtcNow;
        var order = new Order
        {
            Id = Guid.CreateVersion7(),
            CustomerEmail = customerEmail,
            Total = total,
            CreatedUtc = now
        };

        var orderPlaced = new OrderPlaced(order.Id, customerEmail, total, now);

        db.Orders.Add(order);
        db.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.CreateVersion7(),
            Exchange = Topology.Exchange,
            RoutingKey = Topology.OrderPlacedKey,
            Type = nameof(OrderPlaced),
            Payload = JsonSerializer.Serialize(orderPlaced, JsonSerializerOptions.Web),
            OccurredUtc = now
        });

        await db.SaveChangesAsync();

        Log.Info("order-service", $"order {order.Id.ToString()[^8..]} + outbox row saved in ONE transaction");
        return order.Id;
    }
}
