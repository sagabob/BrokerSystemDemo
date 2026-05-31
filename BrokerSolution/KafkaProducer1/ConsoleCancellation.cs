namespace KafkaProducer1;

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

    public static Task WaitForStopKeyAsync(CancellationTokenSource cts)
    {
        return Task.Run(() =>
        {
            try
            {
                Console.ReadKey(intercept: true);

                if (!cts.IsCancellationRequested)
                {
                    cts.Cancel();
                }
            }
            catch (InvalidOperationException)
            {
                // Console input is unavailable when redirected.
            }
        }, CancellationToken.None);
    }
}
