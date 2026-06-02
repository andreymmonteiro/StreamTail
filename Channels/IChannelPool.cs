using RabbitMQ.Client;

namespace StreamTail.Channels;

public interface IChannelPool : IAsyncDisposable
{
    Task<ChannelLease> RentAsync(CancellationToken ct = default);

    Task Return(IChannel channel);
}

