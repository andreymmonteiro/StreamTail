using Moq;
using RabbitMQ.Client;
using StreamTail.Connections;
using StreamTail.Options;

namespace StreamTail.Tests;

public class ConnectionPoolTests
{
    private readonly Mock<IConnectionFactory> _factoryMock = new();
    private readonly Mock<IConnection> _connectionMock = new();

    public ConnectionPoolTests()
    {
        _connectionMock.SetupGet(c => c.IsOpen).Returns(true);
        _connectionMock.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        _factoryMock
            .Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_connectionMock.Object);
    }

    private ConnectionPool CreatePool(ConnectionPoolOptions? options = null) =>
        new(_factoryMock.Object, options ?? new ConnectionPoolOptions
        {
            SweepInterval = TimeSpan.FromHours(1)
        });

    // -----------------------------------------------------------------------
    // GetConnectionAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task GetConnectionAsync_CreatesNewConnection_WhenPoolIsEmpty()
    {
        await using var pool = CreatePool();

        var connection = await pool.GetConnectionAsync();

        Assert.Equal(_connectionMock.Object, connection);
        _factoryMock.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetConnectionAsync_ReusesIdleConnection_AfterReturn()
    {
        await using var pool = CreatePool();

        var first = await pool.GetConnectionAsync();
        await pool.ReturnConnectionAsync(first);

        var second = await pool.GetConnectionAsync();

        Assert.Equal(_connectionMock.Object, second);
        _factoryMock.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetConnectionAsync_CreatesNewConnection_WhenIdleConnectionIsClosed()
    {
        var closedConn = new Mock<IConnection>();
        closedConn.SetupGet(c => c.IsOpen).Returns(false);
        closedConn.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var freshConn = new Mock<IConnection>();
        freshConn.SetupGet(c => c.IsOpen).Returns(true);
        freshConn.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _factoryMock
            .SetupSequence(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(closedConn.Object)
            .ReturnsAsync(freshConn.Object);

        await using var pool = CreatePool();

        // Get the initial (closed) connection, return it as closed
        var conn = await pool.GetConnectionAsync();
        closedConn.SetupGet(c => c.IsOpen).Returns(false);
        await pool.ReturnConnectionAsync(closedConn.Object);

        // Next get must replace the dead one
        var result = await pool.GetConnectionAsync();

        Assert.Equal(freshConn.Object, result);
        closedConn.Verify(c => c.DisposeAsync(), Times.AtLeastOnce);
    }

    [Fact]
    public async Task GetConnectionAsync_ReleasesSlot_WhenConnectionCreationFails()
    {
        var options = new ConnectionPoolOptions
        {
            MaxConnections = 1,
            MaxConnectionCreationAttempts = 1,
            SweepInterval = TimeSpan.FromHours(1)
        };

        _factoryMock
            .SetupSequence(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("broker unreachable"))
            .ReturnsAsync(_connectionMock.Object);

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        await Assert.ThrowsAsync<InvalidOperationException>(() => pool.GetConnectionAsync());

        // Slot must be free — second call should succeed immediately
        var conn = await pool.GetConnectionAsync();
        Assert.Equal(_connectionMock.Object, conn);
    }

    [Fact]
    public async Task GetConnectionAsync_ThrowsOperationCanceledException_WhenCancelled()
    {
        var options = new ConnectionPoolOptions { MaxConnections = 1, SweepInterval = TimeSpan.FromHours(1) };
        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        // Exhaust the single slot
        await pool.GetConnectionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pool.GetConnectionAsync(cts.Token));
    }

    [Fact]
    public async Task GetConnectionAsync_RetriesOnFailure_UpToMaxAttempts()
    {
        var options = new ConnectionPoolOptions
        {
            MaxConnectionCreationAttempts = 3,
            ConnectionCreationRetryDelay = TimeSpan.FromMilliseconds(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        _factoryMock
            .SetupSequence(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("attempt 1"))
            .ThrowsAsync(new InvalidOperationException("attempt 2"))
            .ReturnsAsync(_connectionMock.Object);

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        var conn = await pool.GetConnectionAsync();

        Assert.Equal(_connectionMock.Object, conn);
        _factoryMock.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    // -----------------------------------------------------------------------
    // ReturnConnectionAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task ReturnConnectionAsync_AddsConnectionToIdleQueue_WhenOpen()
    {
        await using var pool = CreatePool();

        var conn = await pool.GetConnectionAsync();
        await pool.ReturnConnectionAsync(conn);

        // Verify the connection is back in the pool by renting again (no new creation)
        var reused = await pool.GetConnectionAsync();
        Assert.Equal(_connectionMock.Object, reused);
        _factoryMock.Verify(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReturnConnectionAsync_DisposesConnection_WhenClosed()
    {
        await using var pool = CreatePool();

        var conn = await pool.GetConnectionAsync();
        _connectionMock.SetupGet(c => c.IsOpen).Returns(false);
        await pool.ReturnConnectionAsync(conn);

        _connectionMock.Verify(c => c.DisposeAsync(), Times.Once);

        _connectionMock.SetupGet(c => c.IsOpen).Returns(true);
    }

    [Fact]
    public async Task ReturnConnectionAsync_ReleasesSlot_WhenConnectionIsClosed()
    {
        var options = new ConnectionPoolOptions { MaxConnections = 1, SweepInterval = TimeSpan.FromHours(1) };

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        var conn = await pool.GetConnectionAsync();
        _connectionMock.SetupGet(c => c.IsOpen).Returns(false);
        await pool.ReturnConnectionAsync(conn);
        _connectionMock.SetupGet(c => c.IsOpen).Returns(true);

        // Slot must be free now
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var next = await pool.GetConnectionAsync(cts.Token);
        Assert.Equal(_connectionMock.Object, next);
    }

    // -----------------------------------------------------------------------
    // DisposeAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task DisposeAsync_DisposesAllIdleConnections()
    {
        var conn1 = new Mock<IConnection>();
        var conn2 = new Mock<IConnection>();
        conn1.SetupGet(c => c.IsOpen).Returns(true);
        conn2.SetupGet(c => c.IsOpen).Returns(true);
        conn1.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        conn2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _factoryMock
            .SetupSequence(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(conn1.Object)
            .ReturnsAsync(conn2.Object);

        var pool = CreatePool();
        var c1 = await pool.GetConnectionAsync();
        var c2 = await pool.GetConnectionAsync();
        await pool.ReturnConnectionAsync(c1);
        await pool.ReturnConnectionAsync(c2);

        await pool.DisposeAsync();

        conn1.Verify(c => c.DisposeAsync(), Times.Once);
        conn2.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_CompletesWithoutHanging()
    {
        var pool = CreatePool();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await pool.DisposeAsync().AsTask().WaitAsync(cts.Token);
    }

    // -----------------------------------------------------------------------
    // SweepAsync (internal)
    // -----------------------------------------------------------------------

    [Fact]
    public async Task SweepAsync_DisposesExpiredConnections_WhileKeepingMinimum()
    {
        var conn1 = new Mock<IConnection>();
        var conn2 = new Mock<IConnection>();
        conn1.SetupGet(c => c.IsOpen).Returns(true);
        conn2.SetupGet(c => c.IsOpen).Returns(true);
        conn1.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        conn2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _factoryMock
            .SetupSequence(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(conn1.Object)
            .ReturnsAsync(conn2.Object);

        var options = new ConnectionPoolOptions
        {
            MaxConnections = 10,
            MinConnections = 1,
            IdleConnectionTimeout = TimeSpan.FromMilliseconds(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        var c1 = await pool.GetConnectionAsync();
        var c2 = await pool.GetConnectionAsync();
        await pool.ReturnConnectionAsync(c1);
        await pool.ReturnConnectionAsync(c2);

        await Task.Delay(20);
        await pool.SweepAsync();

        var disposed =
            (conn1.Invocations.Any(i => i.Method.Name == nameof(IAsyncDisposable.DisposeAsync)) ? 1 : 0) +
            (conn2.Invocations.Any(i => i.Method.Name == nameof(IAsyncDisposable.DisposeAsync)) ? 1 : 0);
        Assert.Equal(1, disposed);
    }

    [Fact]
    public async Task SweepAsync_DoesNotDisposeConnections_WhenCountAtMinimum()
    {
        var options = new ConnectionPoolOptions
        {
            MaxConnections = 10,
            MinConnections = 1,
            IdleConnectionTimeout = TimeSpan.FromMilliseconds(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        var conn = await pool.GetConnectionAsync();
        await pool.ReturnConnectionAsync(conn);

        await Task.Delay(20);
        await pool.SweepAsync();

        _connectionMock.Verify(c => c.DisposeAsync(), Times.Never);
    }

    [Fact]
    public async Task SweepAsync_DoesNotDisposeConnections_WhenNotExpired()
    {
        var conn2 = new Mock<IConnection>();
        conn2.SetupGet(c => c.IsOpen).Returns(true);
        conn2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _factoryMock
            .SetupSequence(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_connectionMock.Object)
            .ReturnsAsync(conn2.Object);

        var options = new ConnectionPoolOptions
        {
            MaxConnections = 10,
            MinConnections = 1,
            IdleConnectionTimeout = TimeSpan.FromHours(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        var c1 = await pool.GetConnectionAsync();
        var c2 = await pool.GetConnectionAsync();
        await pool.ReturnConnectionAsync(c1);
        await pool.ReturnConnectionAsync(c2);

        await pool.SweepAsync();

        _connectionMock.Verify(c => c.DisposeAsync(), Times.Never);
        conn2.Verify(c => c.DisposeAsync(), Times.Never);
    }

    // -----------------------------------------------------------------------
    // Statistics
    // -----------------------------------------------------------------------

    [Fact]
    public async Task GetStatistics_ReturnsCorrectCounts()
    {
        await using var pool = CreatePool();

        var conn = await pool.GetConnectionAsync();

        var stats = pool.GetStatistics();
        Assert.Equal(1, stats.ActiveConnections);
        Assert.Equal(0, stats.IdleConnections);
        Assert.Equal(1, stats.TotalConnectionsCreated);

        await pool.ReturnConnectionAsync(conn);

        stats = pool.GetStatistics();
        Assert.Equal(0, stats.ActiveConnections);
        Assert.Equal(1, stats.IdleConnections);
    }

    [Fact]
    public async Task GetStatistics_TracksDisposedConnections()
    {
        await using var pool = CreatePool();

        var conn = await pool.GetConnectionAsync();
        _connectionMock.SetupGet(c => c.IsOpen).Returns(false);
        await pool.ReturnConnectionAsync(conn);
        _connectionMock.SetupGet(c => c.IsOpen).Returns(true);

        var stats = pool.GetStatistics();
        Assert.Equal(1, stats.TotalConnectionsDisposed);
    }

    // -----------------------------------------------------------------------
    // Concurrent access
    // -----------------------------------------------------------------------

    [Fact]
    public async Task ConcurrentGetConnection_BackpressureWorks_WhenPoolIsFull()
    {
        const int maxConn = 2;
        var options = new ConnectionPoolOptions { MaxConnections = maxConn, SweepInterval = TimeSpan.FromHours(1) };

        int callIndex = 0;
        var connections = Enumerable.Range(0, maxConn + 1).Select(_ =>
        {
            var m = new Mock<IConnection>();
            m.SetupGet(c => c.IsOpen).Returns(true);
            m.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
            return m;
        }).ToList();

        _factoryMock
            .Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => connections[Interlocked.Increment(ref callIndex) - 1].Object);

        await using var pool = new ConnectionPool(_factoryMock.Object, options);

        var leases = await Task.WhenAll(
            Enumerable.Range(0, maxConn).Select(_ => pool.GetConnectionAsync()));

        // Pool is full — next call should block
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var blocked = pool.GetConnectionAsync(cts.Token);
        Assert.False(blocked.IsCompleted);

        // Return one → unblocks
        await pool.ReturnConnectionAsync(leases[0]);
        await Task.Delay(50);
        Assert.True(blocked.IsCompleted);

        var extra = await blocked;
        await pool.ReturnConnectionAsync(extra);
        foreach (var l in leases.Skip(1)) await pool.ReturnConnectionAsync(l);
    }
}
