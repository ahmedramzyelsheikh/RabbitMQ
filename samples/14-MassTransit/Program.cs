using Learning.Shared;
using MassTransit;
using MassTransitDemo;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shop.Contracts;

Log.Title("14 MassTransit on RabbitMQ: publish events, send commands, request/response, retries and faults");

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();

builder.Services.AddSingleton<DemoProgress>();
builder.Services.AddMassTransit(bus =>
{
    bus.SetKebabCaseEndpointNameFormatter();

    bus.AddConsumer<OrderSubmittedConsumer>();
    bus.AddConsumer<SendInvoiceConsumer>();
    bus.AddConsumer<CheckStockConsumer>();
    bus.AddConsumer<OrderSubmittedFaultConsumer>();
    bus.AddRequestClient<CheckStock>(RequestTimeout.After(s: 10));

    bus.UsingRabbitMq((context, rabbit) =>
    {
        var uri = new Uri(Broker.Uri);
        var credentials = uri.UserInfo.Split(':');
        rabbit.Host(uri.Host, (ushort)uri.Port, "/", host =>
        {
            host.Username(Uri.UnescapeDataString(credentials[0]));
            host.Password(Uri.UnescapeDataString(credentials[1]));
        });

        rabbit.UseMessageRetry(retry => retry.Intervals(100, 200, 400));
        rabbit.ConfigureEndpoints(context);
    });
});

using var host = builder.Build();
await host.StartAsync();

var progress = host.Services.GetRequiredService<DemoProgress>();
var publishEndpoint = host.Services.GetRequiredService<IPublishEndpoint>();
var stockClient = host.Services.GetRequiredService<IRequestClient<CheckStock>>();

Log.Info("demo", "request/response: IRequestClient<CheckStock> -> StockStatus");
foreach (var sku in new[] { "SKU-1", "SKU-2" })
{
    var response = await stockClient.GetResponse<StockStatus>(new CheckStock(sku, 3));
    Log.Info("requester", $"{response.Message.Sku}: available={response.Message.Available} (on hand {response.Message.OnHand})");
}

Log.Info("demo", "publish two OrderSubmitted events: one valid, one that always fails");
await publishEndpoint.Publish(new OrderSubmitted(Guid.NewGuid(), "mona@example.com", 450m));
await publishEndpoint.Publish(new OrderSubmitted(Guid.NewGuid(), "thief@example.com", 50_000m));

await progress.Completed.WaitAsync(TimeSpan.FromSeconds(30));

Log.Info("demo", "the failing message was retried 3 times in memory, then moved to 'order-submitted_error' and a Fault<T> was published");
Log.Info("demo", "open http://localhost:15672 to see the exchanges and queues MassTransit created");

await host.StopAsync();
