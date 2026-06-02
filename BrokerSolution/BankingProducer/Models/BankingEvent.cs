namespace BankingProducer.Models;

public sealed class BankingEvent
{
    public required string EventId { get; init; }
    public required string AccountId { get; init; }
    public required int Sequence { get; init; }
    public required string EventType { get; init; }
    public required decimal Amount { get; init; }
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
}
