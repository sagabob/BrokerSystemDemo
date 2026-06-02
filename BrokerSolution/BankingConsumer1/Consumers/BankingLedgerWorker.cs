using BankingConsumer1.Configuration;
using BankingConsumer1.Handlers;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace BankingConsumer1.Consumers;

public sealed class BankingLedgerWorker(
    ILogger<BankingLedgerWorker> logger,
    IOptions<KafkaOptions> options,
    IBankingLedgerHandler ledgerHandler) : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = BuildConsumer();
        consumer.Subscribe(_options.Topic);

        logger.LogInformation(
            "[{ClientId}] Subscribed to {Topic} group={GroupId}",
            _options.ClientId,
            _options.Topic,
            _options.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;

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
                    await ledgerHandler.ProcessAsync(result, stoppingToken);
                    consumer.Commit(result);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "[{ClientId}] Failed to process key={Key} partition={Partition} offset={Offset}",
                        _options.ClientId,
                        result.Message.Key,
                        result.Partition.Value,
                        result.Offset.Value);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("[{ClientId}] Shutdown requested.", _options.ClientId);
        }
        finally
        {
            consumer.Close();
        }
    }

    private IConsumer<string, string> BuildConsumer()
    {
        return new ConsumerBuilder<string, string>(_options.CreateConsumerConfig())
            .SetErrorHandler((_, error) =>
            {
                var level = error.IsFatal ? LogLevel.Critical : LogLevel.Warning;
                logger.Log(level, "Kafka error: {Reason}", error.Reason);
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                logger.LogInformation(
                    "[{ClientId}] Partitions assigned: {Partitions}",
                    _options.ClientId,
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                logger.LogInformation(
                    "[{ClientId}] Partitions revoked: {Partitions}",
                    _options.ClientId,
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .Build();
    }
}
