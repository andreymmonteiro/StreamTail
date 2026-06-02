using RabbitMQ.Client;

namespace StreamTail.Channels;

internal interface IChannelPool : IAsyncDisposable
{
    ValueTask<ChannelLease> RentAsync(CancellationToken ct = default);

    Task Return(IChannel channel);
}

