using Microsoft.Extensions.Configuration;

namespace BankingProducer.Configuration;

public static class AppConfiguration
{
    public static KafkaSettings Load()
    {
        var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
            ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
            ?? "Development";

        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        return configuration.GetSection("Kafka").Get<KafkaSettings>()
            ?? throw new InvalidOperationException("Missing Kafka configuration in appsettings.json.");
    }
}
