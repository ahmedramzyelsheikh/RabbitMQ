using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace LearningTests;

public abstract class BrokerTest(RabbitMqFixture fixture)
{
    protected static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    protected RabbitMqFixture Fixture { get; } = fixture;

    protected static string Unique(string name) => $"{name}.{Guid.NewGuid():N}";

    protected Task<IChannel> ChannelAsync() =>
        Fixture.Connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true));

    protected static async Task<string> QueueAsync(IChannel channel, string name, Dictionary<string, object?>? arguments = null)
    {
        await channel.QueueDeclareAsync(name, durable: true, exclusive: false, autoDelete: false, arguments: arguments);
        return name;
    }

    protected static ValueTask PublishAsync(IChannel channel, string exchange, string routingKey, string body,
        BasicProperties? properties = null, bool mandatory = false) =>
        channel.BasicPublishAsync(exchange, routingKey, mandatory, properties ?? new BasicProperties(), Encoding.UTF8.GetBytes(body));

    protected static async Task<List<string>> DrainAsync(IChannel channel, string queue)
    {
        var bodies = new List<string>();
        while (await channel.BasicGetAsync(queue, autoAck: true) is { } result)
        {
            bodies.Add(Encoding.UTF8.GetString(result.Body.Span));
        }
        return bodies;
    }

    protected static async Task<BasicGetResult> WaitForMessageAsync(IChannel channel, string queue)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await channel.BasicGetAsync(queue, autoAck: true) is { } result)
            {
                return result;
            }
            await Task.Delay(50);
        }
        throw new TimeoutException($"No message arrived in '{queue}' within {Timeout.TotalSeconds}s");
    }

    protected static string Text(ReadOnlyMemory<byte> body) => Encoding.UTF8.GetString(body.Span);

    protected static string? HeaderString(IReadOnlyBasicProperties properties, string key) =>
        properties.Headers is not null && properties.Headers.TryGetValue(key, out var value)
            ? value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : value?.ToString()
            : null;

    protected static IDictionary<string, object?> FirstDeath(IReadOnlyBasicProperties properties)
    {
        Assert.NotNull(properties.Headers);
        var deaths = Assert.IsAssignableFrom<IList<object?>>(properties.Headers["x-death"]);
        return Assert.IsAssignableFrom<IDictionary<string, object?>>(deaths[0]);
    }

    protected static string DeathField(IDictionary<string, object?> death, string key) =>
        death[key] is byte[] bytes ? Encoding.UTF8.GetString(bytes) : death[key]?.ToString() ?? string.Empty;

    protected static async Task<AsyncEventingBasicConsumer> ConsumeAsync(IChannel channel, string queue,
        Func<BasicDeliverEventArgs, Task> handler, bool autoAck = false, IDictionary<string, object?>? arguments = null)
    {
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) => handler(delivery);
        await channel.BasicConsumeAsync(queue, autoAck, consumerTag: string.Empty, noLocal: false, exclusive: false,
            arguments: arguments, consumer: consumer);
        return consumer;
    }
}
