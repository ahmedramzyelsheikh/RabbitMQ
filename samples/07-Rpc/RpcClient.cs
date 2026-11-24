using System.Collections.Concurrent;
using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Rpc;

public enum ReplyMode
{
    ExclusiveQueue,
    DirectReplyTo
}

public sealed class RpcClient : IAsyncDisposable
{
    private const string DirectReplyToQueue = "amq.rabbitmq.reply-to";

    private readonly IChannel channel;
    private readonly string replyTo;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<ReadOnlyMemory<byte>>> pending = new();

    private RpcClient(IChannel channel, string replyTo)
    {
        this.channel = channel;
        this.replyTo = replyTo;
    }

    public static async Task<RpcClient> CreateAsync(IConnection connection, ReplyMode mode)
    {
        var channel = await connection.CreateChannelAsync();

        var replyQueue = mode == ReplyMode.DirectReplyTo
            ? DirectReplyToQueue
            : (await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true, arguments: null)).QueueName;

        var client = new RpcClient(channel, replyQueue);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += client.OnReplyAsync;
        await channel.BasicConsumeAsync(queue: replyQueue, autoAck: true, consumer: consumer);

        return client;
    }

    public async Task DeclareQueueAsync(string queue) =>
        await channel.QueueDeclareAsync(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);

    public async Task<TReply> CallAsync<TRequest, TReply>(string queue, TRequest request, TimeSpan timeout)
    {
        var correlationId = Guid.NewGuid().ToString();
        var reply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        pending[correlationId] = reply;

        var properties = new BasicProperties
        {
            CorrelationId = correlationId,
            ReplyTo = replyTo,
            ContentType = Json.ContentType,
            Expiration = ((int)timeout.TotalMilliseconds).ToString()
        };

        try
        {
            await channel.BasicPublishAsync(exchange: string.Empty, routingKey: queue, mandatory: false,
                basicProperties: properties, body: Json.Serialize(request));

            var body = await reply.Task.WaitAsync(timeout);
            return Json.Deserialize<TReply>(body);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"No reply from '{queue}' within {timeout.TotalSeconds:0.#}s (correlation {correlationId[..8]})");
        }
        finally
        {
            pending.TryRemove(correlationId, out _);
        }
    }

    private Task OnReplyAsync(object sender, BasicDeliverEventArgs delivery)
    {
        var correlationId = delivery.BasicProperties.CorrelationId;
        if (correlationId is not null && pending.TryRemove(correlationId, out var reply))
        {
            reply.TrySetResult(delivery.Body.ToArray());
        }
        else
        {
            Log.Error("rpc-client", $"late or unknown reply {correlationId} ignored");
        }

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await channel.CloseAsync();
        await channel.DisposeAsync();
    }
}
