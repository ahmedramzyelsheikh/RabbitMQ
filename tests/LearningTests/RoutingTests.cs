using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace LearningTests;

public sealed class RoutingTests(RabbitMqFixture fixture) : BrokerTest(fixture)
{
    [Fact]
    public async Task Default_exchange_routes_to_the_queue_named_by_the_routing_key()
    {
        await using var channel = await ChannelAsync();
        var queue = await QueueAsync(channel, Unique("hello"));

        await PublishAsync(channel, string.Empty, queue, "hi");

        Assert.Equal(["hi"], await DrainAsync(channel, queue));
    }

    [Fact]
    public async Task Direct_exchange_needs_an_exact_routing_key_match()
    {
        await using var channel = await ChannelAsync();
        var exchange = Unique("direct");
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Direct);
        var errors = await QueueAsync(channel, Unique("errors"));
        await channel.QueueBindAsync(errors, exchange, "error");

        await PublishAsync(channel, exchange, "error", "disk failure");
        await PublishAsync(channel, exchange, "info", "user login");
        await PublishAsync(channel, exchange, "error.critical", "not an exact match");

        Assert.Equal(["disk failure"], await DrainAsync(channel, errors));
    }

    [Fact]
    public async Task Fanout_exchange_ignores_the_routing_key_and_copies_to_every_bound_queue()
    {
        await using var channel = await ChannelAsync();
        var exchange = Unique("fanout");
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Fanout);
        var first = await QueueAsync(channel, Unique("first"));
        var second = await QueueAsync(channel, Unique("second"));
        await channel.QueueBindAsync(first, exchange, string.Empty);
        await channel.QueueBindAsync(second, exchange, string.Empty);

        await PublishAsync(channel, exchange, "whatever-key", "broadcast");

        Assert.Equal(["broadcast"], await DrainAsync(channel, first));
        Assert.Equal(["broadcast"], await DrainAsync(channel, second));
    }

    [Theory]
    [InlineData("order.*.created", "order.eg.created", true)]
    [InlineData("order.*.created", "order.eg.cairo.created", false)]
    [InlineData("order.*.created", "order.created", false)]
    [InlineData("order.#", "order", true)]
    [InlineData("order.#", "order.eg.payment.failed", true)]
    [InlineData("#.failed", "order.eg.payment.failed", true)]
    [InlineData("*.failed", "order.eg.payment.failed", false)]
    [InlineData("#", "anything.at.all", true)]
    [InlineData("order.eg", "order.eg", true)]
    public async Task Topic_exchange_star_is_one_word_and_hash_is_zero_or_more_words(string pattern, string routingKey, bool delivered)
    {
        await using var channel = await ChannelAsync();
        var exchange = Unique("topic");
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic);
        var queue = await QueueAsync(channel, Unique("topic-queue"));
        await channel.QueueBindAsync(queue, exchange, pattern);

        await PublishAsync(channel, exchange, routingKey, "event");

        Assert.Equal(delivered ? 1 : 0, (await DrainAsync(channel, queue)).Count);
    }

    [Fact]
    public async Task Headers_exchange_x_match_all_versus_any()
    {
        await using var channel = await ChannelAsync();
        var exchange = Unique("headers");
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Headers);
        var all = await QueueAsync(channel, Unique("all"));
        var any = await QueueAsync(channel, Unique("any"));
        await channel.QueueBindAsync(all, exchange, string.Empty, new Dictionary<string, object?> { ["x-match"] = "all", ["format"] = "pdf", ["type"] = "report" });
        await channel.QueueBindAsync(any, exchange, string.Empty, new Dictionary<string, object?> { ["x-match"] = "any", ["format"] = "pdf", ["type"] = "report" });

        await PublishAsync(channel, exchange, string.Empty, "pdf report", new BasicProperties { Headers = new Dictionary<string, object?> { ["format"] = "pdf", ["type"] = "report" } });
        await PublishAsync(channel, exchange, string.Empty, "pdf invoice", new BasicProperties { Headers = new Dictionary<string, object?> { ["format"] = "pdf", ["type"] = "invoice" } });

        Assert.Equal(["pdf report"], await DrainAsync(channel, all));
        Assert.Equal(["pdf report", "pdf invoice"], await DrainAsync(channel, any));
    }

    [Fact]
    public async Task Exchange_to_exchange_binding_forwards_matching_messages()
    {
        await using var channel = await ChannelAsync();
        var source = Unique("source");
        var destination = Unique("destination");
        await channel.ExchangeDeclareAsync(source, ExchangeType.Topic);
        await channel.ExchangeDeclareAsync(destination, ExchangeType.Fanout);
        await channel.ExchangeBindAsync(destination, source, "order.#");
        var queue = await QueueAsync(channel, Unique("e2e"));
        await channel.QueueBindAsync(queue, destination, string.Empty);

        await PublishAsync(channel, source, "order.created", "forwarded");
        await PublishAsync(channel, source, "user.created", "not forwarded");

        Assert.Equal(["forwarded"], await DrainAsync(channel, queue));
    }

    [Fact]
    public async Task Unroutable_message_without_mandatory_is_confirmed_and_silently_dropped()
    {
        await using var channel = await ChannelAsync();
        var exchange = Unique("nobody-bound");
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Direct);

        var exception = await Record.ExceptionAsync(async () => await PublishAsync(channel, exchange, "key", "lost"));

        Assert.Null(exception);
    }

    [Fact]
    public async Task Unroutable_message_with_mandatory_is_returned_to_the_publisher()
    {
        await using var channel = await ChannelAsync();
        var exchange = Unique("nobody-bound");
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Direct);

        var exception = await Assert.ThrowsAsync<PublishReturnException>(async () =>
            await PublishAsync(channel, exchange, "key", "returned", mandatory: true));

        Assert.True(exception.IsReturn);
        Assert.Equal(312, (int)exception.ReplyCode);
        Assert.Equal("NO_ROUTE", exception.ReplyText);
    }

    [Fact]
    public async Task Alternate_exchange_catches_messages_no_binding_matched()
    {
        await using var channel = await ChannelAsync();
        var alternate = Unique("alternate");
        var main = Unique("main");
        await channel.ExchangeDeclareAsync(alternate, ExchangeType.Fanout);
        var unrouted = await QueueAsync(channel, Unique("unrouted"));
        await channel.QueueBindAsync(unrouted, alternate, string.Empty);
        await channel.ExchangeDeclareAsync(main, ExchangeType.Direct, arguments: new Dictionary<string, object?> { ["alternate-exchange"] = alternate });

        await PublishAsync(channel, main, "unknown.key", "rescued", mandatory: true);

        Assert.Equal(["rescued"], await DrainAsync(channel, unrouted));
    }
}
