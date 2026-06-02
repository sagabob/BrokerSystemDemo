using Confluent.Kafka;

namespace BankingProducer.Configuration;

public sealed class KafkaSettings
{
    public required string BootstrapServers { get; init; }
    public required string SaslUsername { get; init; }
    public required string SaslPassword { get; init; }
    public string? CaCertificatePath { get; init; }
    public required string Topic { get; init; }

    public ClientConfig CreateClientConfig()
    {
        var config = new ClientConfig
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.ScramSha256,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword
        };

        if (!string.IsNullOrWhiteSpace(CaCertificatePath))
        {
            config.SslCaPem = File.ReadAllText(ResolveCertificatePath(CaCertificatePath));
        }

        return config;
    }

    private static string ResolveCertificatePath(string certificatePath)
    {
        if (Path.IsPathRooted(certificatePath))
        {
            if (!File.Exists(certificatePath))
            {
                throw new FileNotFoundException($"CA certificate not found at '{certificatePath}'.", certificatePath);
            }

            return certificatePath;
        }

        foreach (var basePath in new[] { AppContext.BaseDirectory, Directory.GetCurrentDirectory() })
        {
            var candidatePath = Path.GetFullPath(Path.Combine(basePath, certificatePath));
            if (File.Exists(candidatePath))
            {
                return candidatePath;
            }
        }

        throw new FileNotFoundException($"CA certificate not found at '{certificatePath}'.");
    }
}
