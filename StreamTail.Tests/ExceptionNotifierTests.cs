using System.Text.Json;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using StreamTail.Channels;
using StreamTail.Logging;

namespace StreamTail.Tests;

public class ExceptionNotifierTests
{
    private readonly Mock<IChannelPool> _poolMock = new();
    private readonly Mock<IChannel> _channelMock = new();
    private readonly Mock<ILogger<ExceptionNotifier>> _loggerMock = new();

    public ExceptionNotifierTests()
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

    private ExceptionNotifier CreateNotifier() =>
        new(_poolMock.Object, _loggerMock.Object);

    [Fact]
    public async Task Notify_UsesQueueNameAsRoutingKey()
    {
        string? capturedRoutingKey = null;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, routingKey, _, _, _, _) => capturedRoutingKey = routingKey)
            .Returns(ValueTask.CompletedTask);

        var notifier = CreateNotifier();

        await notifier.Notify(new Exception("oops"), "my-dlq", "payload", CancellationToken.None);

        Assert.Equal("my-dlq", capturedRoutingKey);
    }

    [Fact]
    public async Task Notify_PublishesToEmptyExchange()
    {
        string? capturedExchange = null;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (exchange, _, _, _, _, _) => capturedExchange = exchange)
            .Returns(ValueTask.CompletedTask);

        await CreateNotifier().Notify(new Exception("err"), "queue", "msg", CancellationToken.None);

        Assert.Equal("", capturedExchange);
    }

    [Fact]
    public async Task Notify_LogsExactlyOnce()
    {
        await CreateNotifier().Notify(new InvalidOperationException("fail"), "dlq", "body", CancellationToken.None);

        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task Notify_IncludesExceptionMessageInBody()
    {
        ReadOnlyMemory<byte> capturedBody = default;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, _, body, _) => capturedBody = body)
            .Returns(ValueTask.CompletedTask);

        var exception = new InvalidOperationException("something went wrong");
        await CreateNotifier().Notify(exception, "dlq", "original-message", CancellationToken.None);

        var doc = JsonDocument.Parse(capturedBody.ToArray());
        Assert.Equal("something went wrong", doc.RootElement.GetProperty("Reason").GetString());
        Assert.Equal("original-message", doc.RootElement.GetProperty("Content").GetString());
    }

    [Fact]
    public async Task Notify_SetsPersistentDeliveryMode()
    {
        BasicProperties? capturedProps = null;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, bool, BasicProperties, ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, props, _, _) => capturedProps = props)
            .Returns(ValueTask.CompletedTask);

        await CreateNotifier().Notify(new Exception("err"), "dlq", "msg", CancellationToken.None);

        Assert.NotNull(capturedProps);
        Assert.True(capturedProps!.Persistent);
    }

    [Fact]
    public async Task Notify_RentsAndReturnsChannel()
    {
        await CreateNotifier().Notify(new Exception("err"), "dlq", "msg", CancellationToken.None);

        _poolMock.Verify(p => p.RentAsync(It.IsAny<CancellationToken>()), Times.Once);
        _poolMock.Verify(p => p.Return(It.IsAny<IChannel>()), Times.Once);
    }
}
