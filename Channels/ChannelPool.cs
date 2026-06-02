using System.Collections.Concurrent;
using System.Diagnostics;
using RabbitMQ.Client;
using StreamTail.Options;

namespace StreamTail.Channels;

public sealed class ChannelPool : IChannelPool
{
    private readonly IConnection _connection;
    private readonly ConcurrentQueue<(IChannel Channel, long LastUse)> _idle = new();
    private readonly TimeSpan _idleCutoff;
    private readonly SemaphoreSlim _slots;
    private readonly Task _sweeper;
    private readonly PeriodicTimer _sweepTimer;
    private readonly CancellationTokenSource _sweepCts;
    private readonly int _minSize;

    public ChannelPool(IConnection connection, ChannelPoolOptions? options = null)
    {
        options ??= new ChannelPoolOptions();
        _connection = connection;
        _minSize = options.MinPoolSize;
        _idleCutoff = options.IdleChannelTimeout;
        _slots = new SemaphoreSlim(options.MaxPoolSize, options.MaxPoolSize);
        _sweepTimer = new PeriodicTimer(options.SweepInterval);
        _sweepCts = new CancellationTokenSource();
        _sweeper = Task.Run(() => SweepWatchAsync(_sweepCts.Token));
    }

    public async Task<ChannelLease> RentAsync(CancellationToken ct = default)
    {
        var channel = await AcquireChannelAsync(ct);
        return new ChannelLease(this, channel);
    }

    // Acquires a channel and its semaphore slot. The caller is responsible for
    // eventually calling Return(channel) to release the slot.
    internal async ValueTask<IChannel> AcquireChannelAsync(CancellationToken ct = default)
    {
        await _slots.WaitAsync(ct);

        try
        {
            if (_idle.TryDequeue(out var tuple))
            {
                if (tuple.Channel.IsOpen)
                    return tuple.Channel;

                try { await tuple.Channel.DisposeAsync(); } catch { }
                return await _connection.CreateChannelAsync(cancellationToken: ct);
            }

            return await _connection.CreateChannelAsync(cancellationToken: ct);
        }
        catch
        {
            _slots.Release();
            throw;
        }
    }

    public async Task Return(IChannel channel)
    {
        if (channel.IsOpen)
        {
            _idle.Enqueue((channel, Stopwatch.GetTimestamp()));
            _slots.Release();
            return;
        }

        try { await channel.DisposeAsync(); } catch { }
        _slots.Release();
    }

    private async Task SweepWatchAsync(CancellationToken ct)
    {
        try
        {
            while (await _sweepTimer.WaitForNextTickAsync(ct))
                await SweepAsync();
        }
        catch (OperationCanceledException) { }
    }

    internal async Task SweepAsync()
    {
        var nowTicks = Stopwatch.GetTimestamp();

        while (_idle.Count > _minSize &&
               _idle.TryPeek(out var head) &&
               TimestampOlderThanCutoff(head.LastUse, nowTicks) &&
               _idle.TryDequeue(out var old))
        {
            try { await old.Channel.DisposeAsync(); } catch { }
        }
    }

    private bool TimestampOlderThanCutoff(long lastUse, long nowTicks) =>
        Stopwatch.GetElapsedTime(lastUse, nowTicks) >= _idleCutoff;

    public async ValueTask DisposeAsync()
    {
        _sweepCts.Cancel();
        try { await _sweeper; } catch { }
        _sweepTimer.Dispose();

        while (_idle.TryDequeue(out var tuple))
        {
            try { await tuple.Channel.DisposeAsync(); } catch { }
        }

        _slots.Dispose();
        _sweepCts.Dispose();
        await _connection.DisposeAsync();
    }
}
