using BankingConsumer1.Configuration;
using BankingConsumer1.Consumers;
using BankingConsumer1.Handlers;

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

builder.Services.AddSingleton<IBankingLedgerHandler, InMemoryBankingLedgerHandler>();
builder.Services.AddHostedService<BankingLedgerWorker>();

await builder.Build().RunAsync();
