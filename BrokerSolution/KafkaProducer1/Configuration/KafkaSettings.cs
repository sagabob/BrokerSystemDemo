using Confluent.Kafka;

namespace KafkaProducer1.Configuration;

public sealed class KafkaSettings
{
    public required string BootstrapServers { get; init; }
    public required string SaslUsername { get; init; }
    public required string SaslPassword { get; init; }
    public string? CaCertificatePath { get; init; }
    public required string Topic { get; init; }
    public required string GroupId { get; init; }

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
            var caPath = ResolveCertificatePath(CaCertificatePath);
            config.SslCaPem = File.ReadAllText(caPath);
        }

        return config;
    }

    private static string ResolveCertificatePath(string certificatePath)
    {
        if (Path.IsPathRooted(certificatePath))
        {
            if (!File.Exists(certificatePath))
            {
                throw new FileNotFoundException(
                    $"CA certificate not found at '{certificatePath}'. Download it from the Aiven console.",
                    certificatePath);
            }

            return certificatePath;
        }

        foreach (var basePath in new[]
                 {
                     AppContext.BaseDirectory,
                     Directory.GetCurrentDirectory()
                 }.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var candidatePath = Path.GetFullPath(Path.Combine(basePath, certificatePath));
            if (File.Exists(candidatePath))
            {
                return candidatePath;
            }
        }

        var expectedPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, certificatePath));
        throw new FileNotFoundException(
            $"CA certificate not found at '{expectedPath}'. Download it from the Aiven console and place it next to the app or project.",
            expectedPath);
    }
}
