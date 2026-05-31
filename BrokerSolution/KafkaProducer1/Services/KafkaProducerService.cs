using Confluent.Kafka;
using KafkaProducer1.Configuration;

namespace KafkaProducer1.Services;

public static class KafkaProducerService
{
    public static async Task ProduceAsync(
        KafkaSettings settings,
        string[] messageArgs,
        CancellationToken cancellationToken = default)
    {
        var messages = messageArgs.Length > 0
            ? messageArgs
            : [$"Hello from .NET at {DateTimeOffset.UtcNow:O}"];

        using var producer = new ProducerBuilder<Null, string>(settings.CreateClientConfig()).Build();

        for (var i = 0; i < messages.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var message = messages[i];
            var deliveryResult = await producer.ProduceAsync(
                settings.Topic,
                new Message<Null, string> { Value = message }, cancellationToken);

            Console.WriteLine(
                $"Message sent successfully [{i + 1}/{messages.Length}] " +
                $"topic={deliveryResult.Topic} " +
                $"partition={deliveryResult.Partition.Value} " +
                $"offset={deliveryResult.Offset.Value} " +
                $"value=\"{message}\"");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine($"Done. {messages.Length} message(s) sent to topic '{settings.Topic}'.");
    }

    public static async Task ProduceStreamAsync(
        KafkaSettings settings,
        CancellationToken cancellationToken = default)
    {
        using var cts = ConsoleCancellation.Create(cancellationToken);
        using var producer = new ProducerBuilder<Null, string>(settings.CreateClientConfig()).Build();

        Console.WriteLine($"Streaming to topic '{settings.Topic}'. Type a message and press Enter.");
        Console.WriteLine("Type 'exit' to stop, or press Ctrl+C.");

        var messageCount = 0;

        while (!cts.Token.IsCancellationRequested)
        {
            Console.Write("> ");
            var input = Console.ReadLine();

            if (cts.Token.IsCancellationRequested)
            {
                break;
            }

            if (input is null || input.Equals("exit", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            if (string.IsNullOrWhiteSpace(input))
            {
                continue;
            }

            messageCount++;
            var deliveryResult = await producer.ProduceAsync(
                settings.Topic,
                new Message<Null, string> { Value = input }, cts.Token);

            Console.WriteLine(
                $"Message sent successfully #{messageCount} " +
                $"topic={deliveryResult.Topic} " +
                $"partition={deliveryResult.Partition.Value} " +
                $"offset={deliveryResult.Offset.Value} " +
                $"value=\"{input}\"");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine($"Stream stopped. {messageCount} message(s) sent to topic '{settings.Topic}'.");
    }

    public static async Task ProduceStreamAutoAsync(
        KafkaSettings settings,
        int intervalSeconds = 1,
        CancellationToken cancellationToken = default)
    {
        if (intervalSeconds < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(intervalSeconds), "Interval must be at least 1 second.");
        }

        using var cts = ConsoleCancellation.Create(cancellationToken);
        using var producer = new ProducerBuilder<Null, string>(settings.CreateClientConfig()).Build();

        Console.WriteLine($"Auto-streaming to topic '{settings.Topic}' every {intervalSeconds}s.");
        Console.WriteLine("Press any key or Ctrl+C to stop.");

        var stopKeyTask = ConsoleCancellation.WaitForStopKeyAsync(cts);
        var messageCount = 0;

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                messageCount++;
                var randomPart = Guid.NewGuid().ToString("N")[..8];
                var message = $"{randomPart} @ {DateTimeOffset.UtcNow:O}";

                var deliveryResult = await producer.ProduceAsync(
                    settings.Topic,
                    new Message<Null, string> { Value = message }, cts.Token);

                Console.WriteLine(
                    $"Message sent successfully #{messageCount} " +
                    $"topic={deliveryResult.Topic} " +
                    $"partition={deliveryResult.Partition.Value} " +
                    $"offset={deliveryResult.Offset.Value} " +
                    $"value=\"{message}\"");

                await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), cts.Token);
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Expected when the user presses a key or Ctrl+C.
        }
        finally
        {
            try
            {
                await stopKeyTask.WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            }
            catch (TimeoutException)
            {
                // Stop listener did not finish in time; process is shutting down anyway.
            }
            catch (OperationCanceledException)
            {
                // Stop listener was cancelled.
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"Auto-stream stopped. {messageCount} message(s) sent to topic '{settings.Topic}'.");
        }
    }
}
