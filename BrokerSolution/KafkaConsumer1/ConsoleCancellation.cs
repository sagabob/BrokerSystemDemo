namespace KafkaConsumer1;

internal static class ConsoleCancellation
{
    public static CancellationTokenSource Create(CancellationToken additionalToken = default)
    {
        var cts = additionalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(additionalToken)
            : new CancellationTokenSource();

        ConsoleCancelEventHandler onCancelKeyPress = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
        };

        Console.CancelKeyPress += onCancelKeyPress;

        if (cts is { Token: { CanBeCanceled: true } })
        {
            cts.Token.Register(() => Console.CancelKeyPress -= onCancelKeyPress);
        }

        return cts;
    }
}
