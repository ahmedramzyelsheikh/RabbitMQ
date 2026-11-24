namespace Shop.Contracts;

public sealed record OrderSubmitted(Guid OrderId, string CustomerEmail, decimal Total);

public sealed record SendInvoice(Guid OrderId, string CustomerEmail);

public sealed record CheckStock(string Sku, int Quantity);

public sealed record StockStatus(string Sku, bool Available, int OnHand);
