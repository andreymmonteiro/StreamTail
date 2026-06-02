using System.Collections.Concurrent;
using RabbitMQ.Client;
using StreamTail.Connections;
using StreamTail.Options;

namespace StreamTail.Channels;

internal sealed class ConnectionAwareChannelPool : IChannelPool
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly ConcurrentDictionary<IConnection, ChannelPool> _channelPools = new();
    private readonly ChannelPoolOptions _channelOptions;

    public ConnectionAwareChannelPool(
        IConnectionProvider connectionProvider,
        ChannelPoolOptions? channelOptions = null)
    {
        _connectionProvider = connectionProvider;
        _channelOptions = channelOptions ?? new ChannelPoolOptions();
    }

    public async ValueTask<ChannelLease> RentAsync(CancellationToken ct = default)
    {
        var connection = await _connectionProvider.GetConnectionAsync(ct);

        try
        {
            RemoveDeadConnectionPools();

            var pool = _channelPools.GetOrAdd(
                connection,
                conn => new ChannelPool(conn, _channelOptions));

            var channel = await pool.AcquireChannelAsync(ct);

            // Return the connection immediately — it is shared, not exclusively held.
            // Multiple leases on the same connection can coexist.
            await _connectionProvider.ReturnConnectionAsync(connection);

            // The lease points to the inner ChannelPool so that Return routes correctly.
            return new ChannelLease(pool, channel);
        }
        catch
        {
            await _connectionProvider.ReturnConnectionAsync(connection);
            throw;
        }
    }

    // Called only if a consumer bypasses ChannelLease and invokes Return directly.
    // In normal use the lease dispatches directly to the inner ChannelPool.Return.
    public Task Return(IChannel channel) => Task.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var pool in _channelPools.Values)
        {
            try { await pool.DisposeAsync(); } catch { }
        }
        _channelPools.Clear();
        await _connectionProvider.DisposeAsync();
    }

    private void RemoveDeadConnectionPools()
    {
        foreach (var kvp in _channelPools)
        {
            if (!kvp.Key.IsOpen && _channelPools.TryRemove(kvp.Key, out var dead))
                _ = dead.DisposeAsync().AsTask();
        }
    }
}
