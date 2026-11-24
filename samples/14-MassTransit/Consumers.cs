using Learning.Shared;
using MassTransit;
using Shop.Contracts;

namespace MassTransitDemo;

public sealed class DemoProgress
{
    public Countdown Completed { get; } = new(3);
}

public sealed class OrderSubmittedConsumer(DemoProgress progress) : IConsumer<OrderSubmitted>
{
    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        var order = context.Message;
        var attempt = context.GetRetryAttempt() + 1;

        if (order.Total > 10_000)
        {
            Log.Error("order-consumer", $"order {order.OrderId.ToString()[..8]} attempt {attempt}: fraud check failed -> throw");
            throw new InvalidOperationException($"Order {order.OrderId} needs manual review");
        }

        Log.Info("order-consumer", $"order {order.OrderId.ToString()[..8]} accepted ({order.Total} EGP), sending SendInvoice command");
        await context.Send(new Uri("queue:send-invoice"), new SendInvoice(order.OrderId, order.CustomerEmail));
        progress.Completed.Signal();
    }
}

public sealed class SendInvoiceConsumer(DemoProgress progress) : IConsumer<SendInvoice>
{
    public Task Consume(ConsumeContext<SendInvoice> context)
    {
        Log.Info("invoice-consumer", $"invoice for order {context.Message.OrderId.ToString()[..8]} emailed to {context.Message.CustomerEmail}");
        progress.Completed.Signal();
        return Task.CompletedTask;
    }
}

public sealed class CheckStockConsumer : IConsumer<CheckStock>
{
    private static readonly Dictionary<string, int> Stock = new() { ["SKU-1"] = 12, ["SKU-2"] = 0 };

    public async Task Consume(ConsumeContext<CheckStock> context)
    {
        var onHand = Stock.GetValueOrDefault(context.Message.Sku);
        Log.Info("stock-consumer", $"stock check {context.Message.Sku} x{context.Message.Quantity}: on hand {onHand}");
        await context.RespondAsync(new StockStatus(context.Message.Sku, onHand >= context.Message.Quantity, onHand));
    }
}

public sealed class OrderSubmittedFaultConsumer(DemoProgress progress) : IConsumer<Fault<OrderSubmitted>>
{
    public Task Consume(ConsumeContext<Fault<OrderSubmitted>> context)
    {
        var reason = context.Message.Exceptions.FirstOrDefault()?.Message;
        Log.Error("fault-consumer", $"order {context.Message.Message.OrderId.ToString()[..8]} faulted after retries: {reason}");
        progress.Completed.Signal();
        return Task.CompletedTask;
    }
}
