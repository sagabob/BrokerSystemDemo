using Confluent.Kafka;

namespace KafkaConsumer2.Handlers;

public sealed class LoggingKafkaMessageHandler(ILogger<LoggingKafkaMessageHandler> logger) : IKafkaMessageHandler
{
    public Task HandleAsync(ConsumeResult<Ignore, string> result, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        logger.LogInformation(
            "Processed message at {Timestamp} topic={Topic} partition={Partition} offset={Offset} value={Value}",
            result.Message.Timestamp.UtcDateTime,
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message.Value);

        return Task.CompletedTask;
    }
}
