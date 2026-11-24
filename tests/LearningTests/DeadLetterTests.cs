using RabbitMQ.Client;

namespace LearningTests;

public sealed class DeadLetterTests(RabbitMqFixture fixture) : BrokerTest(fixture)
{
    private async Task<(string Dlx, string Dlq)> DeadLetterTargetAsync(IChannel channel)
    {
        var dlx = Unique("dlx");
        await channel.ExchangeDeclareAsync(dlx, ExchangeType.Fanout);
        var dlq = await QueueAsync(channel, Unique("dlq"));
        await channel.QueueBindAsync(dlq, dlx, string.Empty);
        return (dlx, dlq);
    }

    [Fact]
    public async Task Rejected_message_is_dead_lettered_with_reason_rejected()
    {
        await using var channel = await ChannelAsync();
        var (dlx, dlq) = await DeadLetterTargetAsync(channel);
        var queue = await QueueAsync(channel, Unique("work"), new() { ["x-dead-letter-exchange"] = dlx });
        await PublishAsync(channel, string.Empty, queue, "poison");

        var delivery = await channel.BasicGetAsync(queue, autoAck: false);
        Assert.NotNull(delivery);
        await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false);

        var dead = await WaitForMessageAsync(channel, dlq);
        var death = FirstDeath(dead.BasicProperties);
        Assert.Equal("poison", Text(dead.Body));
        Assert.Equal("rejected", DeathField(death, "reason"));
        Assert.Equal(queue, DeathField(death, "queue"));
    }

    [Fact]
    public async Task Expired_message_is_dead_lettered_with_reason_expired()
    {
        await using var channel = await ChannelAsync();
        var (dlx, dlq) = await DeadLetterTargetAsync(channel);
        var queue = await QueueAsync(channel, Unique("ttl"), new() { ["x-message-ttl"] = 200, ["x-dead-letter-exchange"] = dlx });

        await PublishAsync(channel, string.Empty, queue, "too late");

        var dead = await WaitForMessageAsync(channel, dlq);
        Assert.Equal("expired", DeathField(FirstDeath(dead.BasicProperties), "reason"));
    }

    [Fact]
    public async Task Overflowing_a_max_length_queue_dead_letters_the_oldest_with_reason_maxlen()
    {
        await using var channel = await ChannelAsync();
        var (dlx, dlq) = await DeadLetterTargetAsync(channel);
        var queue = await QueueAsync(channel, Unique("bounded"), new() { ["x-max-length"] = 2, ["x-dead-letter-exchange"] = dlx });

        foreach (var text in new[] { "1", "2", "3" })
        {
            await PublishAsync(channel, string.Empty, queue, text);
        }

        var dead = await WaitForMessageAsync(channel, dlq);
        Assert.Equal("1", Text(dead.Body));
        Assert.Equal("maxlen", DeathField(FirstDeath(dead.BasicProperties), "reason"));
        Assert.Equal(["2", "3"], await DrainAsync(channel, queue));
    }

    [Fact]
    public async Task Quorum_queue_delivery_limit_stops_an_endless_requeue_loop()
    {
        await using var channel = await ChannelAsync();
        var (dlx, dlq) = await DeadLetterTargetAsync(channel);
        var queue = await QueueAsync(channel, Unique("quorum"), new()
        {
            ["x-queue-type"] = "quorum",
            ["x-delivery-limit"] = 2,
            ["x-dead-letter-exchange"] = dlx
        });
        await PublishAsync(channel, string.Empty, queue, "poison");

        await using var consumerChannel = await Fixture.Connection.CreateChannelAsync();
        await consumerChannel.BasicQosAsync(0, 1, false);
        var deliveries = 0;
        await ConsumeAsync(consumerChannel, queue, async delivery =>
        {
            Interlocked.Increment(ref deliveries);
            await consumerChannel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true);
        });

        var dead = await WaitForMessageAsync(channel, dlq);
        Assert.Equal("delivery_limit", DeathField(FirstDeath(dead.BasicProperties), "reason"));
        Assert.Equal(3, Volatile.Read(ref deliveries));
    }

    [Fact]
    public async Task Ttl_queue_plus_dead_letter_exchange_implements_a_delayed_retry()
    {
        await using var channel = await ChannelAsync();
        var workExchange = Unique("work-x");
        await channel.ExchangeDeclareAsync(workExchange, ExchangeType.Direct);
        var retryExchange = Unique("retry-x");
        await channel.ExchangeDeclareAsync(retryExchange, ExchangeType.Direct);

        var work = await QueueAsync(channel, Unique("work"), new() { ["x-dead-letter-exchange"] = retryExchange });
        await channel.QueueBindAsync(work, workExchange, "job");
        var retry = await QueueAsync(channel, Unique("retry"), new() { ["x-message-ttl"] = 300, ["x-dead-letter-exchange"] = workExchange });
        await channel.QueueBindAsync(retry, retryExchange, "job");

        await PublishAsync(channel, workExchange, "job", "flaky");
        var first = await WaitForMessageAsync(channel, work);
        var started = DateTime.UtcNow;

        await PublishAsync(channel, retryExchange, "job", Text(first.Body));
        var back = await WaitForMessageAsync(channel, work);

        Assert.Equal("flaky", Text(back.Body));
        Assert.True(DateTime.UtcNow - started >= TimeSpan.FromMilliseconds(250));
        Assert.Equal("expired", DeathField(FirstDeath(back.BasicProperties), "reason"));
    }
}
