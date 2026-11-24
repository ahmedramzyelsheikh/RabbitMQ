namespace Learning.Shared;

public sealed class Countdown
{
    private readonly TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int remaining;

    public Countdown(int count)
    {
        remaining = count;
        if (count <= 0)
        {
            done.TrySetResult();
        }
    }

    public int Remaining => Volatile.Read(ref remaining);

    public void Signal()
    {
        if (Interlocked.Decrement(ref remaining) == 0)
        {
            done.TrySetResult();
        }
    }

    public Task WaitAsync(TimeSpan? timeout = null) => done.Task.WaitAsync(timeout ?? TimeSpan.FromSeconds(60));
}
