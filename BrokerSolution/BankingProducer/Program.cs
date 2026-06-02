using BankingProducer.Configuration;
using BankingProducer.Services;

var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "help";
var settings = AppConfiguration.Load();

switch (mode)
{
    case "help" or "--help" or "-h":
        PrintHelp();
        return;
    case "simulate":
        await BankingSimulationProducer.SimulateAsync(settings);
        return;
    default:
        Console.Error.WriteLine($"Unknown mode: {mode}");
        PrintHelp();
        Environment.ExitCode = 1;
        break;
}

static void PrintHelp()
{
    Console.WriteLine("""
        Banking producer demo

        Usage:
          dotnet run -- simulate

        Creates interleaved events for ACC-001 and ACC-002.
        Kafka key = AccountId → each account's events stay in order on one partition.

        Prerequisites:
          Create topic 'banking-ledger-topic' in Aiven (2+ partitions recommended).
        """);
}
