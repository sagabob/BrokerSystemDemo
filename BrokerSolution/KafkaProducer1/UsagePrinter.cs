namespace KafkaProducer1;

public static class UsagePrinter
{
    public static void Print()
    {
        Console.WriteLine("""
            Aiven Kafka producer demo

            Usage:
              dotnet run -- produce [message...]
              dotnet run -- stream
              dotnet run -- stream-auto [seconds]

            stream:
              Sends each line you type to Kafka until you enter "exit".

            stream-auto:
              Sends a random string with timestamp every N seconds (default: 1).
              Press Enter or any key to stop.

            Consumer:
              Run the KafkaConsumer1 project separately:
              dotnet run --project ..\KafkaConsumer1

            Configuration:
              Edit appsettings.json with values from the Aiven console.
              appsettings.Development.json is loaded by default for local development.
              - Connection information -> bootstrap server
              - Users -> username and password
              - Connection information -> CA certificate (set CaCertificatePath to its full path)

            Environment variables override appsettings files, for example:
              Kafka__BootstrapServers
              Kafka__SaslPassword
            """);
    }
}
