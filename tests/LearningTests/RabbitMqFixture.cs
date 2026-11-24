using RabbitMQ.Client;
using Testcontainers.RabbitMq;

[assembly: AssemblyFixture(typeof(LearningTests.RabbitMqFixture))]

namespace LearningTests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer container = new RabbitMqBuilder("rabbitmq:4.2-management").Build();

    public IConnection Connection { get; private set; } = null!;

    public string ConnectionString => container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await container.StartAsync();
        Connection = await NewConnectionAsync("learning-tests");
    }

    public Task<IConnection> NewConnectionAsync(string name) =>
        new ConnectionFactory { Uri = new Uri(ConnectionString), ClientProvidedName = name }.CreateConnectionAsync();

    public async ValueTask DisposeAsync()
    {
        if (Connection is not null)
        {
            await Connection.CloseAsync();
            await Connection.DisposeAsync();
        }

        await container.DisposeAsync();
    }
}
