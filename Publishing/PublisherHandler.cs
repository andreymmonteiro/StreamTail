using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using StreamTail.Channels;
using StreamTail.Events;

namespace StreamTail.Publishing;

internal sealed class PublisherHandler<TEvent> : IPublisherHandler<TEvent>
    where TEvent : IDomainEvent
{
    private readonly IChannelPool _pool;
    private readonly PublisherOptions _options;

    public PublisherHandler(IChannelPool pool, PublisherOptions options)
    {
        _pool = pool;
        _options = options;
    }

    public async Task PublishAsync(TEvent @event, CancellationToken ct = default)
    {
        await using var lease = await _pool.RentAsync(ct);

        var body = _options.Serializer is not null
            ? _options.Serializer(@event)
            : JsonSerializer.SerializeToUtf8Bytes(@event);

        var properties = new BasicProperties { Persistent = _options.Persistent };

        try
        {
            await lease.Channel.BasicPublishAsync(
                exchange: _options.Exchange,
                routingKey: _options.RoutingKey,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: ct);
        }
        catch (PublishException) when (_options.UseConfirms)
        {
            throw new MessageNackedException();
        }
    }
}
