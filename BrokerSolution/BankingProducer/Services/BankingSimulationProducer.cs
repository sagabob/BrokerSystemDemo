using System.Text.Json;
using BankingProducer.Configuration;
using BankingProducer.Models;
using Confluent.Kafka;

namespace BankingProducer.Services;

public static class BankingSimulationProducer
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    public static async Task SimulateAsync(KafkaSettings settings, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        using var producer = new ProducerBuilder<string, string>(settings.CreateClientConfig()).Build();

        var script = BuildSimulationScript();

        Console.WriteLine($"Publishing {script.Count} banking events to topic '{settings.Topic}'.");
        Console.WriteLine("Events use AccountId as Kafka key so each account stays ordered on one partition.");
        Console.WriteLine();

        foreach (var bankingEvent in script)
        {
            cts.Token.ThrowIfCancellationRequested();

            var payload = JsonSerializer.Serialize(bankingEvent, JsonOptions);
            var deliveryResult = await producer.ProduceAsync(
                settings.Topic,
                new Message<string, string>
                {
                    Key = bankingEvent.AccountId,
                    Value = payload
                });

            Console.WriteLine(
                $"Sent {bankingEvent.AccountId} seq={bankingEvent.Sequence,2} {bankingEvent.EventType,-10} " +
                $"${bankingEvent.Amount,8:F2} → partition={deliveryResult.Partition.Value} offset={deliveryResult.Offset.Value}");

            await Task.Delay(500, cts.Token);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine();
        Console.WriteLine("Simulation complete.");
    }

    private static List<BankingEvent> BuildSimulationScript()
    {
        var now = DateTimeOffset.UtcNow;

        return
        [
            Create("ACC-001", 1, "Deposit", 1000m, now),
            Create("ACC-002", 1, "Deposit", 500m, now.AddMilliseconds(10)),
            Create("ACC-001", 2, "Transfer", -150m, now.AddMilliseconds(20)),
            Create("ACC-002", 2, "Deposit", 250m, now.AddMilliseconds(30)),
            Create("ACC-001", 3, "Withdraw", -200m, now.AddMilliseconds(40)),
            Create("ACC-002", 3, "Transfer", -100m, now.AddMilliseconds(50)),
            Create("ACC-001", 4, "Deposit", 75m, now.AddMilliseconds(60)),
            Create("ACC-002", 4, "Withdraw", -50m, now.AddMilliseconds(70))
        ];
    }

    private static BankingEvent Create(
        string accountId, int sequence, string eventType, decimal amount, DateTimeOffset occurredAt)
    {
        return new BankingEvent
        {
            EventId = $"{accountId}-{sequence:D4}-{Guid.NewGuid():N}"[..32],
            AccountId = accountId,
            Sequence = sequence,
            EventType = eventType,
            Amount = amount,
            OccurredAt = occurredAt
        };
    }
}
