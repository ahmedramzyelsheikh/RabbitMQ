using RabbitMQ.Client;

namespace Learning.Shared;

public static class Broker
{
    public const string DefaultUri = "amqp://guest:guest@localhost:5672/";

    public static string Uri => Environment.GetEnvironmentVariable("RABBITMQ_URI") ?? DefaultUri;

    public static ConnectionFactory CreateFactory(string clientName) => new()
    {
        Uri = new Uri(Uri),
        ClientProvidedName = clientName,
        AutomaticRecoveryEnabled = true,
        TopologyRecoveryEnabled = true,
        NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
        RequestedHeartbeat = TimeSpan.FromSeconds(30)
    };

    public static Task<IConnection> ConnectAsync(string clientName, CancellationToken cancellationToken = default) =>
        CreateFactory(clientName).CreateConnectionAsync(cancellationToken);
}
