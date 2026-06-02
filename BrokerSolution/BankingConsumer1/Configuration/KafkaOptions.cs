using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace BankingConsumer1.Configuration;

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

    [Required]
    public string ClientId { get; init; } = string.Empty;

    public ConsumerConfig CreateConsumerConfig()
    {
        var clientConfig = new ClientConfig
        {
            BootstrapServers = BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.ScramSha256,
            SaslUsername = SaslUsername,
            SaslPassword = SaslPassword
        };

        if (!string.IsNullOrWhiteSpace(CaCertificatePath))
        {
            clientConfig.SslCaPem = File.ReadAllText(ResolveCertificatePath(CaCertificatePath));
        }

        return new ConsumerConfig(clientConfig)
        {
            GroupId = GroupId,
            ClientId = ClientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = false
        };
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
