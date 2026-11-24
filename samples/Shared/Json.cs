using System.Text;
using System.Text.Json;

namespace Learning.Shared;

public static class Json
{
    public const string ContentType = "application/json";

    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public static T Deserialize<T>(ReadOnlyMemory<byte> body) =>
        JsonSerializer.Deserialize<T>(body.Span, Options)
        ?? throw new InvalidOperationException($"Body is not a valid {typeof(T).Name}");

    public static string Text(ReadOnlyMemory<byte> body) => Encoding.UTF8.GetString(body.Span);

    public static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);
}
