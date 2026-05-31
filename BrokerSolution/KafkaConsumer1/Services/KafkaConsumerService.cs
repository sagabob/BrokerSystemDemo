using Confluent.Kafka;
using KafkaConsumer1.Configuration;

namespace KafkaConsumer1.Services;

public static class KafkaConsumerService
{
    public static Task ConsumeAsync(
        KafkaSettings settings,
        CancellationToken cancellationToken = default)
    {
        using var cts = ConsoleCancellation.Create(cancellationToken);
        using var consumer = new ConsumerBuilder<Null, string>(settings.CreateConsumerConfig()).Build();
        consumer.Subscribe(settings.Topic);

        Console.WriteLine($"Waiting for messages on topic '{settings.Topic}' (Ctrl+C to stop)...");

        var messageCount = 0;

        try
        {
            while (true)
            {
                var result = consumer.Consume(cts.Token);
                messageCount++;

                Console.WriteLine(
                    $"Message received #{messageCount} " +
                    $"at {result.Message.Timestamp.UtcDateTime:O} " +
                    $"topic={result.Topic} " +
                    $"partition={result.Partition.Value} " +
                    $"offset={result.Offset.Value} " +
                    $"value=\"{result.Message.Value}\"");
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            Console.WriteLine($"Consumer stopped. Total messages received: {messageCount}.");
        }
        finally
        {
            consumer.Close();
        }

        return Task.CompletedTask;
    }
}
