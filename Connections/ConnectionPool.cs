using System.Collections.Concurrent;
using System.Diagnostics;
using RabbitMQ.Client;
using StreamTail.Monitoring;
using StreamTail.Options;

namespace StreamTail.Connections;

internal sealed class ConnectionPool : IConnectionProvider
{
    private readonly IConnectionFactory _factory;
    private readonly ConcurrentQueue<(IConnection Connection, long LastUse)> _idle = new();
    private readonly SemaphoreSlim _slots;
    private readonly PeriodicTimer _sweepTimer;
    private readonly CancellationTokenSource _sweepCts;
    private readonly Task _sweeper;
    private readonly int _maxConnections;
    private readonly int _minConnections;
    private readonly TimeSpan _idleTimeout;
    private readonly int _maxAttempts;
    private readonly TimeSpan _retryDelay;
    private long _totalCreated;
    private long _totalDisposed;

    public ConnectionPool(IConnectionFactory factory, ConnectionPoolOptions? options = null)
    {
        options ??= new ConnectionPoolOptions();
        _factory = factory;
        _maxConnections = options.MaxConnections;
        _minConnections = options.MinConnections;
        _idleTimeout = options.IdleConnectionTimeout;
        _maxAttempts = options.MaxConnectionCreationAttempts;
        _retryDelay = options.ConnectionCreationRetryDelay;
        _slots = new SemaphoreSlim(_maxConnections, _maxConnections);
        _sweepTimer = new PeriodicTimer(options.SweepInterval);
        _sweepCts = new CancellationTokenSource();
        _sweeper = Task.Run(() => SweepWatchAsync(_sweepCts.Token));
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        await _slots.WaitAsync(ct);

        try
        {
            if (_idle.TryDequeue(out var tuple))
            {
                if (tuple.Connection.IsOpen)
                    return tuple.Connection;

                try { await tuple.Connection.DisposeAsync(); } catch { }
                Interlocked.Increment(ref _totalDisposed);
            }

            return await CreateConnectionWithRetryAsync(ct);
        }
        catch
        {
            _slots.Release();
            throw;
        }
    }

    public async Task ReturnConnectionAsync(IConnection connection)
    {
        if (connection.IsOpen)
        {
            _idle.Enqueue((connection, Stopwatch.GetTimestamp()));
            _slots.Release();
            return;
        }

        try { await connection.DisposeAsync(); } catch { }
        Interlocked.Increment(ref _totalDisposed);
        _slots.Release();
    }

    public ConnectionPoolStatistics GetStatistics() => new()
    {
        IdleConnections = _idle.Count,
        ActiveConnections = _maxConnections - _slots.CurrentCount,
        TotalConnectionsCreated = Interlocked.Read(ref _totalCreated),
        TotalConnectionsDisposed = Interlocked.Read(ref _totalDisposed)
    };

    private async Task<IConnection> CreateConnectionWithRetryAsync(CancellationToken ct)
    {
        Exception? last = null;

        for (int attempt = 0; attempt < _maxAttempts; attempt++)
        {
            try
            {
                var connection = await _factory.CreateConnectionAsync(ct);
                Interlocked.Increment(ref _totalCreated);
                return connection;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (attempt < _maxAttempts - 1)
            {
                last = ex;
                await Task.Delay(_retryDelay * (attempt + 1), ct);
            }
        }

        throw last!;
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

        while (_idle.Count > _minConnections &&
               _idle.TryPeek(out var head) &&
               TimestampOlderThanCutoff(head.LastUse, nowTicks) &&
               _idle.TryDequeue(out var old))
        {
            try
            {
                await old.Connection.DisposeAsync();
                Interlocked.Increment(ref _totalDisposed);
            }
            catch { }
        }
    }

    private bool TimestampOlderThanCutoff(long lastUse, long nowTicks) =>
        Stopwatch.GetElapsedTime(lastUse, nowTicks) >= _idleTimeout;

    public async ValueTask DisposeAsync()
    {
        _sweepCts.Cancel();
        try { await _sweeper; } catch { }
        _sweepTimer.Dispose();

        while (_idle.TryDequeue(out var tuple))
        {
            try
            {
                await tuple.Connection.DisposeAsync();
                Interlocked.Increment(ref _totalDisposed);
            }
            catch { }
        }

        _slots.Dispose();
        _sweepCts.Dispose();
    }
}
