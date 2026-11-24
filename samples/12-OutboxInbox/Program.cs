using Learning.Shared;
using Microsoft.EntityFrameworkCore;
using OutboxInbox;
using RabbitMQ.Client;

Log.Title("12 Transactional outbox (producer side) + inbox / idempotent consumer (consumer side)");

var databasePath = Path.Combine(Path.GetTempPath(), "rabbitmq-learning-outbox.db");
var dbFactory = ShopDbContext.Factory(databasePath);
await using (var db = dbFactory())
{
    await db.Database.EnsureDeletedAsync();
    await db.Database.EnsureCreatedAsync();
}
Log.Info("demo", $"SQLite database: {databasePath}");

await using var connection = await Broker.ConnectAsync("12-outbox-inbox");
await using (var setup = await connection.CreateChannelAsync())
{
    await Topology.DeclareAsync(setup);
}

var processed = 0;
var duplicates = 0;
var handled = new Countdown(5);
await using var consumerChannel = await connection.CreateChannelAsync();
var emailConsumer = new EmailConsumer(consumerChannel, dbFactory, isNew =>
{
    if (isNew)
    {
        Interlocked.Increment(ref processed);
    }
    else
    {
        Interlocked.Increment(ref duplicates);
    }
    handled.Signal();
});
await emailConsumer.StartAsync();

await using var publisherChannel = await connection.CreateChannelAsync(
    new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));
var orders = new OrderService(dbFactory);
var dispatcher = new OutboxDispatcher(publisherChannel, dbFactory);

Log.Info("demo", "step 1: place an order and dispatch the outbox normally");
await orders.PlaceOrderAsync("mona@example.com", 450m);
await dispatcher.DispatchPendingAsync();

Log.Info("demo", "step 2: place two orders, the dispatcher crashes after publishing but before saving SentUtc");
await orders.PlaceOrderAsync("omar@example.com", 120m);
await orders.PlaceOrderAsync("sara@example.com", 980m);
try
{
    await dispatcher.DispatchPendingAsync(crashBeforeMarkingSent: true);
}
catch (SimulatedCrashException ex)
{
    Log.Error("outbox", ex.Message);
}

Log.Info("demo", "step 3: the dispatcher restarts and publishes the 'unsent' rows again -> duplicates on the wire");
await dispatcher.DispatchPendingAsync();

await handled.WaitAsync();

await using (var db = dbFactory())
{
    var orderCount = await db.Orders.CountAsync();
    var emailCount = await db.SentEmails.CountAsync();
    var unsent = await db.OutboxMessages.CountAsync(m => m.SentUtc == null);
    Log.Info("demo", $"orders={orderCount}, emails sent={emailCount}, unsent outbox rows={unsent}");
}

Log.Info("demo", $"deliveries={processed + duplicates}: processed={processed}, duplicates skipped={duplicates}");
Log.Info("demo", "outbox = no lost events (at-least-once), inbox = no double side effects (effectively once)");
