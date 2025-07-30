using RabbitMQ.Client;

namespace StreamTail.Channels;

public sealed class ChannelLease : IAsyncDisposable
{
    private readonly IChannelPool _pool;
    public readonly IChannel Channel;

    public ChannelLease(IChannelPool pool, IChannel channel)
    {
        _pool = pool;
        Channel = channel;
    }

    public async ValueTask DisposeAsync()
    {
        await _pool.Return(Channel);
    }
}

