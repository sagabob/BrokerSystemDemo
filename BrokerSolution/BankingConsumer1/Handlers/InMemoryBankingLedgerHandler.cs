using System.Collections.Concurrent;
using System.Text.Json;
using BankingConsumer1.Models;
using Confluent.Kafka;

namespace BankingConsumer1.Handlers;

public interface IBankingLedgerHandler
{
    Task ProcessAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken);
}

public sealed class InMemoryBankingLedgerHandler(
    ILogger<InMemoryBankingLedgerHandler> logger,
    IConfiguration configuration) : IBankingLedgerHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly string _consumerName = configuration["Kafka:ClientId"] ?? "banking-consumer";
    private readonly ConcurrentDictionary<string, byte> _processedEventIds = new();
    private readonly ConcurrentDictionary<string, AccountLedger> _ledgers = new();

    public Task ProcessAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var bankingEvent = JsonSerializer.Deserialize<BankingEvent>(result.Message.Value, JsonOptions)
            ?? throw new InvalidOperationException("Invalid banking event payload.");

        if (bankingEvent.AccountId != result.Message.Key)
        {
            throw new InvalidOperationException(
                $"Key mismatch: key={result.Message.Key}, payload account={bankingEvent.AccountId}");
        }

        if (_processedEventIds.ContainsKey(bankingEvent.EventId))
        {
            logger.LogWarning(
                "[{Consumer}] DUPLICATE ignored {AccountId} seq={Sequence} eventId={EventId}",
                _consumerName,
                bankingEvent.AccountId,
                bankingEvent.Sequence,
                bankingEvent.EventId);
            return Task.CompletedTask;
        }

        var ledger = _ledgers.GetOrAdd(bankingEvent.AccountId, _ => new AccountLedger());
        lock (ledger)
        {
            if (bankingEvent.Sequence != ledger.LastSequence + 1)
            {
                throw new InvalidOperationException(
                    $"Out-of-order event for {bankingEvent.AccountId}: expected seq {ledger.LastSequence + 1}, got {bankingEvent.Sequence}.");
            }

            ledger.Balance += bankingEvent.Amount;
            ledger.LastSequence = bankingEvent.Sequence;
        }

        _processedEventIds[bankingEvent.EventId] = 0;

        logger.LogInformation(
            "[{Consumer}] APPLIED {AccountId} seq={Sequence} {EventType} {Amount:C} balance={Balance:C} partition={Partition} offset={Offset}",
            _consumerName,
            bankingEvent.AccountId,
            bankingEvent.Sequence,
            bankingEvent.EventType,
            bankingEvent.Amount,
            ledger.Balance,
            result.Partition.Value,
            result.Offset.Value);

        return Task.CompletedTask;
    }

    private sealed class AccountLedger
    {
        public int LastSequence { get; set; }
        public decimal Balance { get; set; }
    }
}
