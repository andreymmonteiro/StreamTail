using System.Diagnostics;
using Moq;
using RabbitMQ.Client;
using StreamTail.Channels;
using StreamTail.Options;

namespace StreamTail.Tests;

public class ChannelPoolTests
{
    private readonly Mock<IConnection> _connectionMock = new();
    private readonly Mock<IChannel> _channelMock = new();

    public ChannelPoolTests()
    {
        _channelMock.SetupGet(c => c.IsOpen).Returns(true);
        _channelMock.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        _connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);
        _connectionMock.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
    }

    private ChannelPool CreatePool(ChannelPoolOptions? options = null) =>
        new(_connectionMock.Object, options ?? new ChannelPoolOptions
        {
            SweepInterval = TimeSpan.FromHours(1)
        });

    // -----------------------------------------------------------------------
    // RentAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task RentAsync_CreatesNewChannel_WhenPoolIsEmpty()
    {
        await using var pool = CreatePool();

        await using var lease = await pool.RentAsync();

        Assert.Equal(_channelMock.Object, lease.Channel);
        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RentAsync_ReusesIdleChannel_AfterReturn()
    {
        await using var pool = CreatePool();

        var first = await pool.RentAsync();
        await first.DisposeAsync(); // returns channel to pool

        await using var second = await pool.RentAsync();

        Assert.Equal(_channelMock.Object, second.Channel);
        // CreateChannelAsync called only once — second rent reused the idle channel
        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RentAsync_CreatesNewChannel_WhenIdleChannelIsClosed()
    {
        var closedChannel = new Mock<IChannel>();
        closedChannel.SetupGet(c => c.IsOpen).Returns(false);
        closedChannel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var freshChannel = new Mock<IChannel>();
        freshChannel.SetupGet(c => c.IsOpen).Returns(true);
        freshChannel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _connectionMock
            .SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(closedChannel.Object)
            .ReturnsAsync(freshChannel.Object);

        await using var pool = CreatePool();

        // Rent and get the closed channel, then return it
        var lease = await pool.RentAsync();
        // Simulate the channel going down after being rented
        closedChannel.SetupGet(c => c.IsOpen).Returns(false);
        await pool.Return(closedChannel.Object); // returns closed channel directly

        // Next rent must detect the closed channel and create a fresh one
        await using var lease2 = await pool.RentAsync();

        Assert.Equal(freshChannel.Object, lease2.Channel);
        closedChannel.Verify(c => c.DisposeAsync(), Times.AtLeastOnce);
    }

    [Fact]
    public async Task RentAsync_ReleasesSlot_WhenChannelCreationFails()
    {
        var options = new ChannelPoolOptions { MaxPoolSize = 1, SweepInterval = TimeSpan.FromHours(1) };

        _connectionMock
            .SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("broker down"))
            .ReturnsAsync(_channelMock.Object);

        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // First rent should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() => pool.RentAsync().AsTask());

        // Slot must have been released — second rent should succeed immediately
        await using var lease = await pool.RentAsync();
        Assert.Equal(_channelMock.Object, lease.Channel);
    }

    [Fact]
    public async Task RentAsync_ThrowsOperationCanceledException_WhenCancelled()
    {
        var options = new ChannelPoolOptions { MaxPoolSize = 1, SweepInterval = TimeSpan.FromHours(1) };
        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // Exhaust the single slot
        await using var lease = await pool.RentAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Pool is full — RentAsync must respect cancellation
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pool.RentAsync(cts.Token).AsTask());
    }

    // -----------------------------------------------------------------------
    // Return
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Return_AddsChannelToIdleQueue_WhenOpen()
    {
        await using var pool = CreatePool();

        // Rent to take a slot, then return
        await pool.RentAsync(); // slot consumed, no idle channels
        await pool.Return(_channelMock.Object);

        // If the channel is back in the pool, a second rent reuses it
        await using var lease = await pool.RentAsync();
        Assert.Equal(_channelMock.Object, lease.Channel);
        _connectionMock.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Return_DisposesChannel_WhenClosed()
    {
        await using var pool = CreatePool();

        // Rent a channel (slot consumed), then simulate it going down
        var lease = await pool.RentAsync();
        _channelMock.SetupGet(c => c.IsOpen).Returns(false);

        // Dispose the lease — Return sees IsOpen=false and disposes the channel
        await lease.DisposeAsync();

        _channelMock.Verify(c => c.DisposeAsync(), Times.Once);

        // Restore for the pool's own DisposeAsync
        _channelMock.SetupGet(c => c.IsOpen).Returns(true);
    }

    [Fact]
    public async Task Return_ReleasesSlot_WhenChannelIsClosed()
    {
        var options = new ChannelPoolOptions { MaxPoolSize = 1, SweepInterval = TimeSpan.FromHours(1) };
        var closedChannel = new Mock<IChannel>();
        closedChannel.SetupGet(c => c.IsOpen).Returns(false);
        closedChannel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // Use the single slot, then return a dead channel
        await pool.RentAsync();
        await pool.Return(closedChannel.Object);

        // Slot must be free now — this should NOT block
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await using var lease = await pool.RentAsync(cts.Token);
        Assert.Equal(_channelMock.Object, lease.Channel);
    }

    // -----------------------------------------------------------------------
    // DisposeAsync
    // -----------------------------------------------------------------------

    [Fact]
    public async Task DisposeAsync_DisposesAllIdleChannels()
    {
        var ch1 = new Mock<IChannel>();
        var ch2 = new Mock<IChannel>();
        ch1.SetupGet(c => c.IsOpen).Returns(true);
        ch2.SetupGet(c => c.IsOpen).Returns(true);
        ch1.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        ch2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _connectionMock
            .SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ch1.Object)
            .ReturnsAsync(ch2.Object);

        var pool = CreatePool();
        var lease1 = await pool.RentAsync();
        var lease2 = await pool.RentAsync();
        await lease1.DisposeAsync();
        await lease2.DisposeAsync();

        await pool.DisposeAsync();

        ch1.Verify(c => c.DisposeAsync(), Times.Once);
        ch2.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_CompletesWithoutHanging()
    {
        var pool = CreatePool();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await pool.DisposeAsync().AsTask().WaitAsync(cts.Token);
    }

    // -----------------------------------------------------------------------
    // SweepAsync (internal, tested via InternalsVisibleTo)
    // -----------------------------------------------------------------------

    [Fact]
    public async Task SweepAsync_DisposesExpiredChannels_WhileKeepingMinimum()
    {
        var ch1 = new Mock<IChannel>();
        var ch2 = new Mock<IChannel>();
        ch1.SetupGet(c => c.IsOpen).Returns(true);
        ch2.SetupGet(c => c.IsOpen).Returns(true);
        ch1.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        ch2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _connectionMock
            .SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ch1.Object)
            .ReturnsAsync(ch2.Object);

        var options = new ChannelPoolOptions
        {
            MaxPoolSize = 10,
            MinPoolSize = 1,
            IdleChannelTimeout = TimeSpan.FromMilliseconds(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // Rent both to create them, then return — they land in the idle queue
        var lease1 = await pool.RentAsync();
        var lease2 = await pool.RentAsync();
        await lease1.DisposeAsync();
        await lease2.DisposeAsync();

        await Task.Delay(20); // let the timestamps age past 1 ms

        await pool.SweepAsync();

        // MinPoolSize=1: exactly one channel should have been disposed (the first in FIFO order)
        var disposed = (ch1.Invocations.Any(i => i.Method.Name == nameof(IAsyncDisposable.DisposeAsync)) ? 1 : 0) +
                       (ch2.Invocations.Any(i => i.Method.Name == nameof(IAsyncDisposable.DisposeAsync)) ? 1 : 0);
        Assert.Equal(1, disposed);
    }

    [Fact]
    public async Task SweepAsync_DoesNotDisposeChannels_WhenCountAtMinimum()
    {
        var options = new ChannelPoolOptions
        {
            MaxPoolSize = 10,
            MinPoolSize = 1,
            IdleChannelTimeout = TimeSpan.FromMilliseconds(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // Rent and return one channel — it lands in the idle queue
        var lease = await pool.RentAsync();
        await lease.DisposeAsync();

        await Task.Delay(20);
        await pool.SweepAsync();

        // count=1 equals MinPoolSize=1: nothing should be disposed
        _channelMock.Verify(c => c.DisposeAsync(), Times.Never);
    }

    [Fact]
    public async Task SweepAsync_DoesNotDisposeChannels_WhenNotExpired()
    {
        var ch2 = new Mock<IChannel>();
        ch2.SetupGet(c => c.IsOpen).Returns(true);
        ch2.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        _connectionMock
            .SetupSequence(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object)
            .ReturnsAsync(ch2.Object);

        var options = new ChannelPoolOptions
        {
            MaxPoolSize = 10,
            MinPoolSize = 1,
            IdleChannelTimeout = TimeSpan.FromHours(1),
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // Rent and return two channels — both are fresh (not expired)
        var lease1 = await pool.RentAsync();
        var lease2 = await pool.RentAsync();
        await lease1.DisposeAsync();
        await lease2.DisposeAsync();

        await pool.SweepAsync();

        _channelMock.Verify(c => c.DisposeAsync(), Times.Never);
        ch2.Verify(c => c.DisposeAsync(), Times.Never);
    }

    // -----------------------------------------------------------------------
    // Concurrent access
    // -----------------------------------------------------------------------

    [Fact]
    public async Task ConcurrentRent_BackpressureWorks_WhenPoolIsFull()
    {
        const int poolSize = 3;
        var options = new ChannelPoolOptions { MaxPoolSize = poolSize, SweepInterval = TimeSpan.FromHours(1) };

        var channels = Enumerable.Range(0, poolSize + 1).Select(_ =>
        {
            var m = new Mock<IChannel>();
            m.SetupGet(c => c.IsOpen).Returns(true);
            m.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
            return m;
        }).ToList();

        int callIndex = 0;
        _connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => channels[Interlocked.Increment(ref callIndex) - 1].Object);

        await using var pool = new ChannelPool(_connectionMock.Object, options);

        // Acquire all slots
        var leases = await Task.WhenAll(Enumerable.Range(0, poolSize).Select(_ => pool.RentAsync().AsTask()));

        // Pool is full — next rent should block
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var blocked = pool.RentAsync(cts.Token).AsTask();
        Assert.False(blocked.IsCompleted);

        // Return one lease → blocked rent should now complete
        await leases[0].DisposeAsync();
        await Task.Delay(50); // let the async machinery run
        Assert.True(blocked.IsCompleted);

        var extraLease = await blocked;
        await extraLease.DisposeAsync();
        foreach (var l in leases.Skip(1)) await l.DisposeAsync();
    }
}
