using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using StreamTail.Channels;

namespace StreamTail.Logging;

internal sealed class ExceptionNotifier : IExceptionNotifier
{
    private readonly IChannelPool _pool;
    private readonly ILogger<ExceptionNotifier> _logger;

    public ExceptionNotifier(IChannelPool pool, ILogger<ExceptionNotifier> logger)
    {
        _pool = pool;
        _logger = logger;
    }

    public async Task Notify(Exception exception, string exchange, string dlqName, string message, CancellationToken cancellationToken)
    {
        _logger.LogError(exception, "Failed to process message, sending to DLQ: {DlqName}", dlqName);

        await using var lease = await _pool.RentAsync(cancellationToken);

        var properties = new BasicProperties { Persistent = true };

        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            FailedAt = DateTime.UtcNow,
            Reason = exception.Message,
            Content = message
        });

        await lease.Channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: dlqName,
            mandatory: true,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);
    }
}
