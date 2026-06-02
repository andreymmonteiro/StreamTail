using System.Text.Json;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using StreamTail.Channels;
using StreamTail.Events;
using StreamTail.Publishing;

namespace StreamTail.UnitTests;

public class PublisherHandlerTests
{
    private readonly Mock<IChannelPool> _poolMock = new();
    private readonly Mock<IChannel> _channelMock = new();

    public PublisherHandlerTests()
    {
        _channelMock.SetupGet(c => c.IsOpen).Returns(true);
        _channelMock.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var lease = new ChannelLease(_poolMock.Object, _channelMock.Object);
        _poolMock
            .Setup(p => p.RentAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(lease);
        _poolMock
            .Setup(p => p.Return(It.IsAny<IChannel>()))
            .Returns(Task.CompletedTask);
    }

    private record TestEvent(string Data) : IDomainEvent;

    private IPublisherHandler<TestEvent> CreateHandler(PublisherOptions? options = null) =>
        new PublisherHandler<TestEvent>(_poolMock.Object, options ?? new PublisherOptions
        {
            Exchange = "test-exchange",
            RoutingKey = "test.key"
        });

    // -----------------------------------------------------------------------
    // Routing
    // -----------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_PublishesToConfiguredExchangeAndRoutingKey()
    {
        string? capturedExchange = null, capturedRoutingKey = null;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (ex, rk, _, _, _, _) => { capturedExchange = ex; capturedRoutingKey = rk; })
            .Returns(ValueTask.CompletedTask);

        await CreateHandler().PublishAsync(new TestEvent("hello"));

        Assert.Equal("test-exchange", capturedExchange);
        Assert.Equal("test.key", capturedRoutingKey);
    }

    // -----------------------------------------------------------------------
    // Serialization
    // -----------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_SerializesEventAsJson_ByDefault()
    {
        ReadOnlyMemory<byte> capturedBody = default;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, _, body, _) => capturedBody = body)
            .Returns(ValueTask.CompletedTask);

        await CreateHandler().PublishAsync(new TestEvent("payload"));

        var doc = JsonDocument.Parse(capturedBody.ToArray());
        Assert.Equal("payload", doc.RootElement.GetProperty("Data").GetString());
    }

    [Fact]
    public async Task PublishAsync_UsesCustomSerializer_WhenProvided()
    {
        var customBytes = new byte[] { 1, 2, 3 };
        ReadOnlyMemory<byte> capturedBody = default;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, _, body, _) => capturedBody = body)
            .Returns(ValueTask.CompletedTask);

        var options = new PublisherOptions
        {
            Exchange = "x",
            RoutingKey = "rk",
            Serializer = _ => customBytes
        };

        await CreateHandler(options).PublishAsync(new TestEvent("ignored"));

        Assert.Equal(customBytes, capturedBody.ToArray());
    }

    // -----------------------------------------------------------------------
    // Message properties
    // -----------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_SetsPersistentDeliveryMode_ByDefault()
    {
        BasicProperties? capturedProps = null;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, props, _, _) => capturedProps = props)
            .Returns(ValueTask.CompletedTask);

        await CreateHandler().PublishAsync(new TestEvent("e"));

        Assert.NotNull(capturedProps);
        Assert.True(capturedProps!.Persistent);
    }

    [Fact]
    public async Task PublishAsync_SetsNonPersistent_WhenConfigured()
    {
        BasicProperties? capturedProps = null;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, props, _, _) => capturedProps = props)
            .Returns(ValueTask.CompletedTask);

        var options = new PublisherOptions { Exchange = "x", RoutingKey = "rk", Persistent = false };
        await CreateHandler(options).PublishAsync(new TestEvent("e"));

        Assert.NotNull(capturedProps);
        Assert.False(capturedProps!.Persistent);
    }

    // -----------------------------------------------------------------------
    // Channel lifecycle
    // -----------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_RentsAndReturnsChannel()
    {
        await CreateHandler().PublishAsync(new TestEvent("e"));

        _poolMock.Verify(p => p.RentAsync(It.IsAny<CancellationToken>()), Times.Once);
        _poolMock.Verify(p => p.Return(It.IsAny<IChannel>()), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Publisher confirms
    // In RabbitMQ.Client v7, BasicPublishAsync throws PublishException on Nack
    // when the channel was created with PublisherConfirmationsEnabled = true.
    // UseConfirms = true converts that exception into MessageNackedException.
    // -----------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_ThrowsMessageNackedException_WhenPublishExceptionAndUseConfirmsTrue()
    {
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Throws(new PublishException(1ul, isReturn: false));

        var options = new PublisherOptions { Exchange = "x", RoutingKey = "rk", UseConfirms = true };

        await Assert.ThrowsAsync<MessageNackedException>(
            () => CreateHandler(options).PublishAsync(new TestEvent("e")));
    }

    [Fact]
    public async Task PublishAsync_PropagatesPublishException_WhenUseConfirmsFalse()
    {
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Throws(new PublishException(1ul, isReturn: false));

        // UseConfirms = false: PublishException is not caught, propagates as-is
        await Assert.ThrowsAsync<PublishException>(
            () => CreateHandler().PublishAsync(new TestEvent("e")));
    }

    [Fact]
    public async Task PublishAsync_SucceedsWithoutException_WhenBrokerAcks()
    {
        // BasicPublishAsync returns normally (ack) — no exception thrown
        var options = new PublisherOptions { Exchange = "x", RoutingKey = "rk", UseConfirms = true };

        var ex = await Record.ExceptionAsync(() => CreateHandler(options).PublishAsync(new TestEvent("e")));

        Assert.Null(ex);
    }
}
