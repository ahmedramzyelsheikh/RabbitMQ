using System.Collections.Concurrent;
using RabbitMQ.Client;

namespace LearningTests;

public sealed class AcknowledgementTests(RabbitMqFixture fixture) : BrokerTest(fixture)
{
    [Fact]
    public async Task Unacked_message_is_redelivered_with_the_redelivered_flag_when_the_consumer_channel_closes()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("unacked"));
        await PublishAsync(channel, string.Empty, queue, "job");

        var consumerChannel = await Fixture.Connection.CreateChannelAsync();
        var firstDelivery = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await ConsumeAsync(consumerChannel, queue, delivery =>
        {
            firstDelivery.TrySetResult(delivery.Redelivered);
            return Task.CompletedTask;
        });
        Assert.False(await firstDelivery.Task.WaitAsync(Timeout));

        await consumerChannel.CloseAsync();
        await consumerChannel.DisposeAsync();

        var again = await WaitForMessageAsync(channel, queue);
        Assert.Equal("job", Text(again.Body));
        Assert.True(again.Redelivered);
    }

    [Fact]
    public async Task Nack_with_requeue_puts_the_message_back_and_reject_without_requeue_drops_it()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("nack"));
        await PublishAsync(channel, string.Empty, queue, "retry me");

        var first = await channel.BasicGetAsync(queue, autoAck: false);
        Assert.NotNull(first);
        await channel.BasicNackAsync(first.DeliveryTag, multiple: false, requeue: true);

        var second = await WaitForMessageAsync(channel, queue);
        Assert.True(second.Redelivered);

        await PublishAsync(channel, string.Empty, queue, "drop me");
        var third = await channel.BasicGetAsync(queue, autoAck: false);
        Assert.NotNull(third);
        await channel.BasicRejectAsync(third.DeliveryTag, requeue: false);

        Assert.Equal(0u, await channel.MessageCountAsync(queue));
    }

    [Fact]
    public async Task Prefetch_limits_how_many_unacked_messages_a_consumer_holds()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("prefetch"));
        for (var i = 0; i < 5; i++)
        {
            await PublishAsync(channel, string.Empty, queue, $"m{i}");
        }

        await using var consumerChannel = await Fixture.Connection.CreateChannelAsync();
        await consumerChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 2, global: false);
        var received = new ConcurrentBag<string>();
        await ConsumeAsync(consumerChannel, queue, delivery =>
        {
            received.Add(Text(delivery.Body));
            return Task.CompletedTask;
        });

        await Task.Delay(1_000);

        Assert.Equal(2, received.Count);
        Assert.Equal(3u, await channel.MessageCountAsync(queue));
    }

    [Fact]
    public async Task Competing_consumers_share_the_messages_of_one_queue()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("competing"));
        var byConsumer = new ConcurrentDictionary<string, int>();
        var all = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var total = 0;

        var channels = new List<IChannel>();
        foreach (var name in new[] { "a", "b" })
        {
            var consumerChannel = await Fixture.Connection.CreateChannelAsync();
            channels.Add(consumerChannel);
            await consumerChannel.BasicQosAsync(0, 1, false);
            await ConsumeAsync(consumerChannel, queue, async delivery =>
            {
                byConsumer.AddOrUpdate(name, 1, (_, n) => n + 1);
                await Task.Delay(20);
                await consumerChannel.BasicAckAsync(delivery.DeliveryTag, false);
                if (Interlocked.Increment(ref total) == 10)
                {
                    all.TrySetResult();
                }
            });
        }

        for (var i = 0; i < 10; i++)
        {
            await PublishAsync(channel, string.Empty, queue, $"task {i}");
        }

        await all.Task.WaitAsync(Timeout);
        Assert.Equal(10, byConsumer.Values.Sum());
        Assert.All(byConsumer.Values, count => Assert.True(count > 0));

        foreach (var consumerChannel in channels)
        {
            await consumerChannel.DisposeAsync();
        }
    }
}
