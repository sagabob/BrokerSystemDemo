using Confluent.Kafka;
using KafkaConsumer2.Configuration;
using KafkaConsumer2.Handlers;
using Microsoft.Extensions.Options;

namespace KafkaConsumer2.Consumers;

public sealed class KafkaConsumerWorker(
    ILogger<KafkaConsumerWorker> logger,
    IOptions<KafkaOptions> options,
    IKafkaMessageHandler messageHandler) : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = BuildConsumer();

        consumer.Subscribe(_options.Topic);
        logger.LogInformation(
            "Subscribed to topic {Topic} with group {GroupId}",
            _options.Topic,
            _options.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string> result;

                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex) when (ex.Error.IsFatal)
                {
                    logger.LogCritical(ex, "Fatal Kafka consume error: {Reason}", ex.Error.Reason);
                    throw;
                }

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                try
                {
                    await messageHandler.HandleAsync(result, stoppingToken);

                    if (!_options.EnableAutoCommit)
                    {
                        consumer.Commit(result);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "Failed to process message topic={Topic} partition={Partition} offset={Offset}",
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value);

                    // Do not commit — Kafka will redeliver after rebalance/restart.
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Shutdown requested. Closing Kafka consumer.");
        }
        finally
        {
            consumer.Close();
        }
    }

    private IConsumer<Ignore, string> BuildConsumer()
    {
        return new ConsumerBuilder<Ignore, string>(_options.CreateConsumerConfig())
            .SetErrorHandler((_, error) =>
            {
                if (error.IsFatal)
                {
                    logger.LogCritical("Kafka fatal error: {Reason}", error.Reason);
                }
                else
                {
                    logger.LogWarning("Kafka error: {Reason}", error.Reason);
                }
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                logger.LogInformation(
                    "Partitions assigned: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                logger.LogInformation(
                    "Partitions revoked: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .Build();
    }
}
