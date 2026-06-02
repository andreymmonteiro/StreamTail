using RabbitMQ.Client;
using StreamTail.Monitoring;

namespace StreamTail.Connections;

internal interface IConnectionProvider : IAsyncDisposable
{
    Task<IConnection> GetConnectionAsync(CancellationToken ct = default);
    Task ReturnConnectionAsync(IConnection connection);
    ConnectionPoolStatistics GetStatistics();
}
