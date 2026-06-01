namespace StreamTail.Monitoring;

public sealed class ConnectionPoolStatistics
{
    public required int IdleConnections { get; init; }
    public required int ActiveConnections { get; init; }
    public required long TotalConnectionsCreated { get; init; }
    public required long TotalConnectionsDisposed { get; init; }
}
