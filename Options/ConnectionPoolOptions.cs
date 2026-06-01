namespace StreamTail.Options;

public sealed class ConnectionPoolOptions
{
    public int MaxConnections { get; set; } = 3;
    public int MinConnections { get; set; } = 1;
    public TimeSpan IdleConnectionTimeout { get; set; } = TimeSpan.FromMinutes(10);
    public TimeSpan SweepInterval { get; set; } = TimeSpan.FromMinutes(2);
    public int MaxConnectionCreationAttempts { get; set; } = 3;
    public TimeSpan ConnectionCreationRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}
