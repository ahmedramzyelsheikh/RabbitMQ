using Learning.Shared;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string QueueName = "task_queue";

var mode = args.FirstOrDefault() ?? "demo";

await using var connection = await Broker.ConnectAsync($"02-work-queues-{mode}");

switch (mode)
{
    case "send":
    {
        await using var channel = await OpenChannelAsync(connection);
        var count = args.Length > 1 ? int.Parse(args[1]) : 10;
        for (var i = 1; i <= count; i++)
        {
            await SendTaskAsync(channel, $"task {i} ...");
        }
        break;
    }
    case "worker":
    {
        var name = args.Length > 1 ? args[1] : $"worker-{Environment.ProcessId}";
        await using var channel = await OpenChannelAsync(connection);
        await StartWorkerAsync(channel, name, TimeSpan.FromMilliseconds(300), _ => { });
        await Shutdown.WaitForCtrlCAsync();
        break;
    }
    default:
        await RunDemoAsync(connection);
        break;
}

static async Task RunDemoAsync(IConnection connection)
{
    Log.Title("02 Work queues: competing consumers, manual ack, prefetch = 1 (fair dispatch)");

    await using var producer = await OpenChannelAsync(connection);
    await producer.QueuePurgeAsync(QueueName);

    const int totalTasks = 12;
    var completed = new Countdown(totalTasks);
    var perWorker = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
    void Done(string worker)
    {
        perWorker.AddOrUpdate(worker, 1, (_, n) => n + 1);
        completed.Signal();
    }

    Log.Info("demo", "step 1: a worker takes a task and crashes before acknowledging it");
    var crashChannel = await OpenChannelAsync(connection);
    await crashChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
    var crashed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var crashConsumer = new AsyncEventingBasicConsumer(crashChannel);
    crashConsumer.ReceivedAsync += (_, delivery) =>
    {
        Log.Error("crashy-worker", $"got '{Json.Text(delivery.Body)}' and crashes without ack");
        crashed.TrySetResult();
        return Task.CompletedTask;
    };
    await crashChannel.BasicConsumeAsync(QueueName, autoAck: false, consumer: crashConsumer);
    await SendTaskAsync(producer, "task 1 ...");
    await crashed.Task.WaitAsync(TimeSpan.FromSeconds(10));

    Log.Info("demo", "step 2: a fast and a slow worker join, more tasks arrive");
    await using var fast = await OpenChannelAsync(connection);
    await using var slow = await OpenChannelAsync(connection);
    await StartWorkerAsync(fast, "fast-worker", TimeSpan.FromMilliseconds(100), Done);
    await StartWorkerAsync(slow, "slow-worker", TimeSpan.FromMilliseconds(600), Done);

    for (var i = 2; i <= totalTasks; i++)
    {
        await SendTaskAsync(producer, $"task {i} ...");
    }

    Log.Info("demo", "step 3: the crashed worker's channel closes, its unacked task goes back to the queue");
    await crashChannel.CloseAsync();
    await crashChannel.DisposeAsync();

    await completed.WaitAsync();
    foreach (var (worker, count) in perWorker.OrderBy(p => p.Key))
    {
        Log.Info("demo", $"{worker} completed {count} tasks");
    }
    Log.Info("demo", "the fast worker did more work because prefetch = 1 only gives a new task after an ack");
}

static async Task<IChannel> OpenChannelAsync(IConnection connection)
{
    var channel = await connection.CreateChannelAsync();
    await channel.QueueDeclareAsync(queue: QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
    return channel;
}

static async Task SendTaskAsync(IChannel channel, string task)
{
    var properties = new BasicProperties
    {
        Persistent = true,
        ContentType = "text/plain",
        MessageId = Guid.NewGuid().ToString()
    };

    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: QueueName, mandatory: false,
        basicProperties: properties, body: Json.Bytes(task));
    Log.Info("producer", $"sent '{task}'");
}

static async Task StartWorkerAsync(IChannel channel, string name, TimeSpan timePerDot, Action<string> onDone)
{
    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        var task = Json.Text(delivery.Body);
        var redelivered = delivery.Redelivered ? " (REDELIVERED)" : string.Empty;
        Log.Info(name, $"working on '{task}'{redelivered}");

        var dots = task.Count(c => c == '.');
        await Task.Delay(timePerDot * dots);

        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
        Log.Info(name, $"done '{task}' -> ack");
        onDone(name);
    };

    await channel.BasicConsumeAsync(queue: QueueName, autoAck: false, consumer: consumer);
}
