using System.Text;
using RabbitMQ.Client;

namespace Learning.Shared;

public static class MessageHeaders
{
    public static string? GetString(IReadOnlyBasicProperties properties, string key) =>
        properties.Headers is not null && properties.Headers.TryGetValue(key, out var value)
            ? AsString(value)
            : null;

    public static string? AsString(object? value) => value switch
    {
        null => null,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        _ => value.ToString()
    };

    public static long DeathCount(IReadOnlyBasicProperties properties, string queue, string? reason = null)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue("x-death", out var raw)
            || raw is not IList<object?> deaths)
        {
            return 0;
        }

        long total = 0;
        foreach (var death in deaths.OfType<IDictionary<string, object?>>())
        {
            var deathQueue = death.TryGetValue("queue", out var q) ? AsString(q) : null;
            var deathReason = death.TryGetValue("reason", out var r) ? AsString(r) : null;
            if (deathQueue == queue && (reason is null || deathReason == reason)
                && death.TryGetValue("count", out var c) && c is long count)
            {
                total += count;
            }
        }

        return total;
    }

    public static string Describe(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null || properties.Headers.Count == 0)
        {
            return "(no headers)";
        }

        return string.Join(", ", properties.Headers.Select(h => $"{h.Key}={Format(h.Value)}"));
    }

    private static string Format(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        IList<object?> list => $"[{string.Join("; ", list.Select(Format))}]",
        IDictionary<string, object?> table => $"{{{string.Join(", ", table.Select(t => $"{t.Key}={Format(t.Value)}"))}}}",
        AmqpTimestamp timestamp => DateTimeOffset.FromUnixTimeSeconds(timestamp.UnixTime).ToString("O"),
        _ => value?.ToString() ?? "null"
    };
}
