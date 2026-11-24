namespace Learning.Shared;

public static class Shutdown
{
    public static Task WaitForCtrlCAsync()
    {
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stopped.TrySetResult();
        };
        Log.Info("app", "running, press Ctrl+C to stop");
        return stopped.Task;
    }
}
