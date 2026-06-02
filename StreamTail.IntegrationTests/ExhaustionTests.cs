using Moq;
using RabbitMQ.Client;
using StreamTail.Channels;
using StreamTail.Connections;
using StreamTail.Options;

namespace StreamTail.IntegrationTests;

public class ExhaustionTests
{
    [Fact]
    public async Task MockedConnections_ExhaustChannelsAndConnections_HandledGracefully()
    {
        // Arrange: create a connection provider that creates a fixed small number of connections
        var factoryMock = new Mock<IConnectionFactory>();

        // Prepare a small pool: max connections 2, channel pool max size 2
        var connMocks = new List<Mock<IConnection>>();
        var channelMocks = new List<Mock<IChannel>>();

        for (int i = 0; i < 2; i++)
        {
            var ch = new Mock<IChannel>();
            ch.SetupGet(c => c.IsOpen).Returns(true);
            ch.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
            channelMocks.Add(ch);

            var conn = new Mock<IConnection>();
            conn.SetupGet(c => c.IsOpen).Returns(true);
            conn.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
            // CreateChannelAsync returns a fresh channel from the list
            var idx = i; // capture
            conn.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(channelMocks[idx].Object);
            connMocks.Add(conn);
        }

        int createIndex = 0;
        factoryMock.Setup(f => f.CreateConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => connMocks[Interlocked.Increment(ref createIndex) - 1].Object);

        var connectionProvider = new ConnectionPool(factoryMock.Object, new ConnectionPoolOptions
        {
            MaxConnections = 2,
            MinConnections = 1,
            SweepInterval = TimeSpan.FromSeconds(10)
        });

        var channelOptions = new ChannelPoolOptions
        {
            MaxPoolSize = 2,
            MinPoolSize = 1,
            SweepInterval = TimeSpan.FromSeconds(10),
            IdleChannelTimeout = TimeSpan.FromMilliseconds(100)
        };

        await using var pool = new ConnectionAwareChannelPool(connectionProvider, channelOptions);


        var tasks = new List<Task<ChannelLease>>();

        for (int i = 0; i < 5; i++) 
        {
            tasks.Add(pool.RentAsync().AsTask());
        }

        await foreach(var completed in Task.WhenEach(tasks)) 
        {
            var result = await completed;

            await result.DisposeAsync();
        }

    }

    [Fact]
    public async Task LocalRabbitMq_ExhaustionTest_HandlesGracefully()
    {
        // This test expects a RabbitMQ instance on localhost:5672
        var factory = new ConnectionFactory { HostName = "localhost", UserName = "amlora", Password = "amlora" };

        var connectionProvider = new ConnectionPool(factory, new ConnectionPoolOptions
        {
            MaxConnections = 2,
            MinConnections = 1,
            SweepInterval = TimeSpan.FromHours(1)
        });

        var channelOptions = new ChannelPoolOptions
        {
            MaxPoolSize = 2,
            MinPoolSize = 1,
            SweepInterval = TimeSpan.FromHours(1)
        };

        await using var pool = new ConnectionAwareChannelPool(connectionProvider, channelOptions);

        // Try to rent more leases than channels to force creation of multiple connections and channels
        var tasks = Enumerable.Range(0, 4).Select(_ => pool.RentAsync().AsTask()).ToArray();

        // Wait with a timeout to avoid hanging CI if rabbitmq not available
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var results = await Task.WhenAll(tasks.Select(t => t.WaitAsync(cts.Token).ContinueWith(_ => t.Result)));

        // Dispose leases
        foreach (var lease in results) await lease.DisposeAsync();

    }
}
