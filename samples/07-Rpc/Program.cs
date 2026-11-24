using Learning.Shared;
using Rpc;

Log.Title("07 RPC / request-reply: replyTo + correlationId");

await using var connection = await Broker.ConnectAsync("07-rpc");

await using var server = await ShippingQuoteServer.StartAsync(connection);

Log.Info("demo", "client A: private exclusive reply queue, 5 concurrent requests");
await using (var client = await RpcClient.CreateAsync(connection, ReplyMode.ExclusiveQueue))
{
    var requests = new[]
    {
        new QuoteRequest("EG", 1.5m), new QuoteRequest("SA", 3m), new QuoteRequest("AE", 0.4m),
        new QuoteRequest("EG", 12m), new QuoteRequest("US", 2m)
    };

    var replies = await Task.WhenAll(requests.Select(r =>
        client.CallAsync<QuoteRequest, QuoteReply>(ShippingQuoteServer.QueueName, r, TimeSpan.FromSeconds(5))));

    foreach (var reply in replies)
    {
        Log.Info("client-A", $"quote {reply.Country} {reply.WeightKg}kg = {reply.Price} EGP");
    }
}

Log.Info("demo", "client B: direct reply-to (amq.rabbitmq.reply-to), no reply queue at all");
await using (var client = await RpcClient.CreateAsync(connection, ReplyMode.DirectReplyTo))
{
    var reply = await client.CallAsync<QuoteRequest, QuoteReply>(ShippingQuoteServer.QueueName, new QuoteRequest("EG", 5m), TimeSpan.FromSeconds(5));
    Log.Info("client-B", $"quote {reply.Country} {reply.WeightKg}kg = {reply.Price} EGP");
}

Log.Info("demo", "client C: calling a queue nobody serves -> the caller must time out");
await using (var client = await RpcClient.CreateAsync(connection, ReplyMode.DirectReplyTo))
{
    await client.DeclareQueueAsync("rpc.nobody-home");
    try
    {
        await client.CallAsync<QuoteRequest, QuoteReply>("rpc.nobody-home", new QuoteRequest("EG", 1m), TimeSpan.FromSeconds(2));
    }
    catch (TimeoutException ex)
    {
        Log.Error("client-C", ex.Message);
    }
}

Log.Info("demo", "done: RPC over a broker works, but always use timeouts and prefer async events when you can");
