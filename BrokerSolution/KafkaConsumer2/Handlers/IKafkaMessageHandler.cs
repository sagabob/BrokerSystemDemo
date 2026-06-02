using Confluent.Kafka;

namespace KafkaConsumer2.Handlers;

public interface IKafkaMessageHandler
{
    Task HandleAsync(ConsumeResult<Ignore, string> result, CancellationToken cancellationToken);
}
