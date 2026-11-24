using System.Collections.Concurrent;

namespace Learning.Shared;

public static class Log
{
    private static readonly ConsoleColor[] Palette =
    [
        ConsoleColor.Cyan, ConsoleColor.Yellow, ConsoleColor.Green, ConsoleColor.Magenta,
        ConsoleColor.Blue, ConsoleColor.DarkYellow, ConsoleColor.DarkCyan, ConsoleColor.DarkGreen
    ];

    private static readonly ConcurrentDictionary<string, ConsoleColor> Colors = new();
    private static readonly Lock Gate = new();

    public static void Title(string text)
    {
        lock (Gate)
        {
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine();
            Console.WriteLine($"=== {text} ===");
            Console.ResetColor();
        }
    }

    public static void Info(string who, string message)
    {
        var color = Colors.GetOrAdd(who, _ => Palette[Colors.Count % Palette.Length]);
        lock (Gate)
        {
            Console.ForegroundColor = color;
            Console.Write($"[{DateTime.Now:HH:mm:ss.fff}] {who,-16} ");
            Console.ResetColor();
            Console.WriteLine(message);
        }
    }

    public static void Error(string who, string message)
    {
        lock (Gate)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {who,-16} {message}");
            Console.ResetColor();
        }
    }
}
