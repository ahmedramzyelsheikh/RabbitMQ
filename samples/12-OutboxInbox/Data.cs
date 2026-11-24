using Microsoft.EntityFrameworkCore;

namespace OutboxInbox;

public sealed record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total, DateTime PlacedUtc);

public sealed class Order
{
    public Guid Id { get; set; }
    public string CustomerEmail { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public DateTime CreatedUtc { get; set; }
}

public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string Exchange { get; set; } = string.Empty;
    public string RoutingKey { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime OccurredUtc { get; set; }
    public DateTime? SentUtc { get; set; }
    public int Attempts { get; set; }
    public string? LastError { get; set; }
}

public sealed class InboxMessage
{
    public string MessageId { get; set; } = string.Empty;
    public string Consumer { get; set; } = string.Empty;
    public DateTime ProcessedUtc { get; set; }
}

public sealed class SentEmail
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public string To { get; set; } = string.Empty;
    public DateTime SentUtc { get; set; }
}

public sealed class ShopDbContext(DbContextOptions<ShopDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    public DbSet<SentEmail> SentEmails => Set<SentEmail>();

    public static Func<ShopDbContext> Factory(string path)
    {
        var options = new DbContextOptionsBuilder<ShopDbContext>()
            .UseSqlite($"Data Source={path}")
            .Options;
        return () => new ShopDbContext(options);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>().HasIndex(m => m.SentUtc);
        modelBuilder.Entity<InboxMessage>().HasKey(m => new { m.MessageId, m.Consumer });
        modelBuilder.Entity<SentEmail>().HasIndex(m => m.OrderId).IsUnique();
    }
}
