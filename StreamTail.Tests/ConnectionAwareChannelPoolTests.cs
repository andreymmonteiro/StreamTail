using Moq;
using RabbitMQ.Client;
using StreamTail.Channels;
using StreamTail.Connections;
using StreamTail.Options;

namespace StreamTail.Tests;

public class ConnectionAwareChannelPoolTests
{
    private readonly Mock<IConnectionProvider> _providerMock = new();
    private readonly Mock<IConnection> _connectionMock = new();
    private readonly Mock<IChannel> _channelMock = new();

    public ConnectionAwareChannelPoolTests()
    {
        _channelMock.SetupGet(c => c.IsOpen).Returns(true);
        _channelMock.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _connectionMock.SetupGet(c => c.IsOpen).Returns(true);
        _connectionMock.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        _connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);

        _providerMock
            .Setup(p => p.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_connectionMock.Object);
        _providerMock
            .Setup(p => p.ReturnConnectionAsync(It.IsAny<IConnection>()))
            .Returns(Task.CompletedTask);
        _providerMock
            .Setup(p => p.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
    }

    private ChannelPoolOptions NoSweepOptions => new()
    {
        SweepInterval = TimeSpan.FromHours(1)
    };

    private ConnectionAwareChannelPool CreatePool() =>
        new(_providerMock.Object, NoSweepOptions);

    // -----------------------------------------------------------------------
    // RentAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task RentAsync_CreatesChannelViaConnectionFromProvider()
    {
        await using var pool = CreatePool();

        await using var lease = await pool.RentAsync();

        Assert.Equal(_channelMock.Object, lease.Channel);
        _providerMock.Verify(p => p.GetConnectionAsync(It.IsAny<CancellationToken>()), Times.Once);
        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RentAsync_ReturnsConnectionToProviderImmediately()
    {
        await using var pool = CreatePool();

        await using var lease = await pool.RentAsync();

        // Connection must be returned to the provider during RentAsync, not on lease dispose
        _providerMock.Verify(p => p.ReturnConnectionAsync(_connectionMock.Object), Times.Once);
    }

    [Fact]
    public async Task RentAsync_ReusesInnerChannelPool_ForSameConnection()
    {
        await using var pool = CreatePool();

        var lease1 = await pool.RentAsync();
        await lease1.DisposeAsync(); // channel returned to inner pool

        await using var lease2 = await pool.RentAsync();

        // Inner pool reuses the channel — only one CreateChannelAsync call
        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RentAsync_CreatesDistinctInnerPools_ForDifferentConnections()
    {
        var conn2 = new Mock<IConnection>();
        conn2.SetupGet(c => c.IsOpen).Returns(true);
        conn2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        conn2.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);

        _providerMock
            .SetupSequence(p => p.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_connectionMock.Object)
            .ReturnsAsync(conn2.Object);

        await using var pool = CreatePool();

        await using var lease1 = await pool.RentAsync();
        await using var lease2 = await pool.RentAsync();

        // Each connection created one channel — total of 2 CreateChannelAsync calls
        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
        conn2.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RentAsync_ReturnsConnectionToProvider_EvenOnChannelCreationFailure()
    {
        _connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("channel creation failed"));

        await using var pool = CreatePool();

        await Assert.ThrowsAsync<InvalidOperationException>(() => pool.RentAsync().AsTask());

        // Connection must still be returned to the provider
        _providerMock.Verify(p => p.ReturnConnectionAsync(_connectionMock.Object), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Channel lease lifecycle
    // -----------------------------------------------------------------------

    [Fact]
    public async Task ChannelLease_ReturnsChannelToInnerPool_OnDispose()
    {
        await using var pool = CreatePool();

        var lease = await pool.RentAsync();
        await lease.DisposeAsync();

        // The channel was returned to the inner pool; renting again reuses it
        await using var lease2 = await pool.RentAsync();
        Assert.Equal(_channelMock.Object, lease2.Channel);

        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ChannelLease_DisposesClosedChannel_OnReturn()
    {
        await using var pool = CreatePool();

        var lease = await pool.RentAsync();

        // Channel goes down while rented
        _channelMock.SetupGet(c => c.IsOpen).Returns(false);
        await lease.DisposeAsync();

        _channelMock.Verify(c => c.DisposeAsync(), Times.Once);

        _channelMock.SetupGet(c => c.IsOpen).Returns(true);
    }

    // -----------------------------------------------------------------------
    // DisposeAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task DisposeAsync_DisposesConnectionProvider()
    {
        var pool = CreatePool();

        await pool.DisposeAsync();

        _providerMock.Verify(p => p.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_DisposesAllInnerChannelPools()
    {
        var pool = CreatePool();

        // Rent to create the inner pool
        var lease = await pool.RentAsync();
        await lease.DisposeAsync();

        await pool.DisposeAsync();

        // The idle channel in the inner pool should have been disposed during pool shutdown
        _channelMock.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_CompletesWithoutHanging()
    {
        var pool = CreatePool();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await pool.DisposeAsync().AsTask().WaitAsync(cts.Token);
    }

    // -----------------------------------------------------------------------
    // Dead connection cleanup
    // -----------------------------------------------------------------------

    [Fact]
    public async Task RentAsync_CleansUpDeadConnectionPool_OnNextRent()
    {
        var conn2 = new Mock<IConnection>();
        conn2.SetupGet(c => c.IsOpen).Returns(true);
        conn2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        conn2.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);

        _providerMock
            .SetupSequence(p => p.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_connectionMock.Object) // first rent
            .ReturnsAsync(conn2.Object);           // second rent (after first dies)

        await using var pool = CreatePool();

        // First rent uses _connectionMock
        var lease1 = await pool.RentAsync();
        await lease1.DisposeAsync();

        // Simulate first connection dying
        _connectionMock.SetupGet(c => c.IsOpen).Returns(false);

        // Second rent — the dead pool for _connectionMock should be cleaned up
        await using var lease2 = await pool.RentAsync();

        // verify we got a channel from conn2
        conn2.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
