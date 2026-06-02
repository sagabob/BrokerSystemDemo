using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace KafkaConsumer2.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required]
    public string BootstrapServers { get; init; } = string.Empty;

    [Required]
    public string SaslUsername { get; init; } = string.Empty;

    [Required]
    public string SaslPassword { get; init; } = string.Empty;

    public string? CaCertificatePath { get; init; }

    [Required]
    public string Topic { get; init; } = string.Empty;

    [Required]
    public string GroupId { get; init; } = string.Empty;

    public string ClientId { get; init; } = "kafka-consumer2";

    public string AutoOffsetReset { get; init; } = "Earliest";

    public bool EnableAutoCommit { get; init; }

    [Range(1000, 600000)]
    public int SessionTimeoutMs { get; init; } = 45000;

    [Range(1000, 3600000)]
    public int MaxPollIntervalMs { get; init; } = 300000;

    public ConsumerConfig CreateConsumerConfig()
    {
        var config = new ConsumerConfig(CreateClientConfig())
        {
            GroupId = GroupId,
            ClientId = ClientId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(AutoOffsetReset, ignoreCase: true),
            EnableAutoCommit = EnableAutoCommit,
            EnablePartitionEof = false,
            SessionTimeoutMs = SessionTimeoutMs,
            MaxPollIntervalMs = MaxPollIntervalMs
        };

        return config;
    }

    private ClientConfig CreateClientConfig()
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
            $"CA certificate not found at '{expectedPath}'. Download it from the Aiven console.",
            expectedPath);
    }
}
