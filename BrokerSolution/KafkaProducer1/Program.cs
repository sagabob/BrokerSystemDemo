using KafkaProducer1;
using KafkaProducer1.Configuration;
using KafkaProducer1.Services;

var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "help";
var settings = AppConfiguration.Load();

switch (mode)
{
    case "help" or "--help" or "-h":
        UsagePrinter.Print();
        return;
    case "produce":
        await KafkaProducerService.ProduceAsync(settings, args.Skip(1).ToArray());
        return;
    case "stream":
        await KafkaProducerService.ProduceStreamAsync(settings);
        return;
    case "stream-auto":
    {
        var intervalSeconds = 1;
        if (args.Length > 1 && !int.TryParse(args[1], out intervalSeconds))
        {
            Console.Error.WriteLine($"Invalid interval: {args[1]}. Use an integer number of seconds.");
            Environment.ExitCode = 1;
            return;
        }

        await KafkaProducerService.ProduceStreamAutoAsync(settings, intervalSeconds);
        return;
    }
}

Console.Error.WriteLine($"Unknown mode: {mode}");
UsagePrinter.Print();
Environment.ExitCode = 1;
