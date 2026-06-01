using RabbitMQ.Client;
using StreamTail.Monitoring;

namespace StreamTail.Connections;

public interface IConnectionProvider : IAsyncDisposable
{
    Task<IConnection> GetConnectionAsync(CancellationToken ct = default);
    Task ReturnConnectionAsync(IConnection connection);
    ConnectionPoolStatistics GetStatistics();
}
