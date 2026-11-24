using System.Collections.Concurrent;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace LearningTests;

public sealed class QueueFeatureTests(RabbitMqFixture fixture) : BrokerTest(fixture)
{
    [Fact]
    public async Task Priority_queue_delivers_higher_priority_first()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("priority"), new() { ["x-max-priority"] = 10 });

        await PublishAsync(channel, string.Empty, queue, "low", new BasicProperties { Priority = 1 });
        await PublishAsync(channel, string.Empty, queue, "high", new BasicProperties { Priority = 9 });
        await PublishAsync(channel, string.Empty, queue, "medium", new BasicProperties { Priority = 5 });

        Assert.Equal(["high", "medium", "low"], await DrainAsync(channel, queue));
    }

    [Fact]
    public async Task Per_message_expiration_only_affects_that_message()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("expiration"));

        await PublishAsync(channel, string.Empty, queue, "short", new BasicProperties { Expiration = "100" });
        await PublishAsync(channel, string.Empty, queue, "long");
        await Task.Delay(400);

        Assert.Equal(["long"], await DrainAsync(channel, queue));
    }

    [Fact]
    public async Task Reject_publish_overflow_nacks_the_publisher_when_the_queue_is_full()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("full"), new() { ["x-max-length"] = 1, ["x-overflow"] = "reject-publish" });

        await PublishAsync(channel, string.Empty, queue, "fits");
        var exception = await Assert.ThrowsAsync<PublishException>(async () => await PublishAsync(channel, string.Empty, queue, "rejected"));

        Assert.False(exception.IsReturn);
        Assert.Equal(["fits"], await DrainAsync(channel, queue));
    }

    [Fact]
    public async Task Redeclaring_a_queue_with_different_arguments_fails_with_406_and_closes_the_channel()
    {
        var name = Unique("conflict");
        await using (var setup = await ChannelAsync())
        {
            await QueueAsync(setup, name);
        }

        await using var channel = await ChannelAsync();
        var exception = await Assert.ThrowsAnyAsync<OperationInterruptedException>(() =>
            channel.QueueDeclareAsync(name, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-message-ttl"] = 1000 }));

        Assert.Equal(406, (int?)exception.ShutdownReason?.ReplyCode);
        Assert.False(channel.IsOpen);
    }

    [Fact]
    public async Task Exclusive_queue_cannot_be_used_from_another_connection()
    {
        await using var owner = await Fixture.Connection.CreateChannelAsync();
        var declared = await owner.QueueDeclareAsync(string.Empty, durable: false, exclusive: true, autoDelete: true);

        await using var otherConnection = await Fixture.NewConnectionAsync("intruder");
        await using var intruder = await otherConnection.CreateChannelAsync();
        var exception = await Assert.ThrowsAnyAsync<OperationInterruptedException>(() =>
            intruder.BasicGetAsync(declared.QueueName, autoAck: true));

        Assert.StartsWith("amq.gen-", declared.QueueName);
        Assert.Equal(405, (int?)exception.ShutdownReason?.ReplyCode);
    }

    [Fact]
    public async Task Single_active_consumer_sends_everything_to_one_consumer()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("sac"), new() { ["x-single-active-consumer"] = true });
        var byConsumer = new ConcurrentDictionary<string, int>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var total = 0;

        var channels = new List<IChannel>();
        foreach (var name in new[] { "first", "second" })
        {
            var consumerChannel = await Fixture.Connection.CreateChannelAsync();
            channels.Add(consumerChannel);
            await ConsumeAsync(consumerChannel, queue, async delivery =>
            {
                byConsumer.AddOrUpdate(name, 1, (_, n) => n + 1);
                await consumerChannel.BasicAckAsync(delivery.DeliveryTag, false);
                if (Interlocked.Increment(ref total) == 6)
                {
                    done.TrySetResult();
                }
            });
        }

        for (var i = 0; i < 6; i++)
        {
            await PublishAsync(channel, string.Empty, queue, $"event {i}");
        }

        await done.Task.WaitAsync(Timeout);
        Assert.Single(byConsumer);
        Assert.Equal(6, byConsumer["first"]);

        foreach (var consumerChannel in channels)
        {
            await consumerChannel.DisposeAsync();
        }
    }

    [Fact]
    public async Task Stream_can_be_replayed_from_any_offset_and_reading_does_not_remove_messages()
    {
        await using var channel = await ChannelAsync();
        var stream = await QueueAsync(channel, Unique("stream"), new() { ["x-queue-type"] = "stream" });
        for (var i = 0; i < 6; i++)
        {
            await PublishAsync(channel, string.Empty, stream, $"e{i}");
        }

        async Task<List<string>> ReadAsync(object offset, int expected)
        {
            await using var reader = await Fixture.Connection.CreateChannelAsync();
            await reader.BasicQosAsync(0, 100, false);
            var bodies = new ConcurrentQueue<string>();
            var complete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await ConsumeAsync(reader, stream, async delivery =>
            {
                bodies.Enqueue(Text(delivery.Body));
                await reader.BasicAckAsync(delivery.DeliveryTag, false);
                if (bodies.Count == expected)
                {
                    complete.TrySetResult();
                }
            }, arguments: new Dictionary<string, object?> { ["x-stream-offset"] = offset });
            await complete.Task.WaitAsync(Timeout);
            return [.. bodies];
        }

        Assert.Equal(["e0", "e1", "e2", "e3", "e4", "e5"], await ReadAsync("first", 6));
        Assert.Equal(["e4", "e5"], await ReadAsync(4L, 2));
        Assert.Equal(["e0", "e1", "e2", "e3", "e4", "e5"], await ReadAsync("first", 6));
    }
}
