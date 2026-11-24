using Learning.Shared;
using OrdersApi;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

await using var connection = await Broker.ConnectAsync("13-orders-api");
await using (var setup = await connection.CreateChannelAsync())
{
    await ShopTopology.DeclareAsync(setup);
}

builder.Services.AddSingleton(connection);
builder.Services.AddSingleton<EventPublisher>();
builder.Services.AddHostedService<NotificationWorker>();

var app = builder.Build();

app.MapGet("/", () => Results.Text("""
    POST /orders   {"customerEmail":"mona@example.com","total":450}
    GET  /health
    RabbitMQ UI    http://localhost:15672 (guest / guest)
    """));

app.MapPost("/orders", async (CreateOrderRequest request, EventPublisher publisher, CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerEmail) || request.Total <= 0)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["order"] = ["customerEmail is required and total must be positive"]
        });
    }

    var orderCreated = new OrderCreated(Guid.CreateVersion7(), request.CustomerEmail, request.Total, DateTime.UtcNow);
    await publisher.PublishAsync(ShopTopology.OrderCreatedKey, orderCreated, cancellationToken);

    return Results.Accepted($"/orders/{orderCreated.OrderId}", new { orderCreated.OrderId, status = "accepted" });
});

app.MapGet("/health", (IConnection rabbit) => rabbit.IsOpen
    ? Results.Ok(new { rabbitmq = "up" })
    : Results.Problem("RabbitMQ connection is closed", statusCode: StatusCodes.Status503ServiceUnavailable));

await app.RunAsync();

public sealed record CreateOrderRequest(string CustomerEmail, decimal Total);
