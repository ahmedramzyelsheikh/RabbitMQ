namespace Rpc;

public sealed record QuoteRequest(string Country, decimal WeightKg);

public sealed record QuoteReply(string Country, decimal WeightKg, decimal Price, string ServedBy);
