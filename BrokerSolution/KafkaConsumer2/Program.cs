using KafkaConsumer2.Configuration;
using KafkaConsumer2.Consumers;
using KafkaConsumer2.Handlers;

var builder = Host.CreateApplicationBuilder(args);

if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT"))
    && string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")))
{
    builder.Environment.EnvironmentName = Environments.Development;
}

builder.Services
    .AddOptions<KafkaOptions>()
    .BindConfiguration(KafkaOptions.SectionName)
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IKafkaMessageHandler, LoggingKafkaMessageHandler>();
builder.Services.AddHostedService<KafkaConsumerWorker>();

var host = builder.Build();
await host.RunAsync();
