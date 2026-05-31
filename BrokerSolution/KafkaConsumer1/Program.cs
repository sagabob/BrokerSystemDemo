using KafkaConsumer1.Configuration;
using KafkaConsumer1.Services;

var settings = AppConfiguration.Load();
await KafkaConsumerService.ConsumeAsync(settings);
