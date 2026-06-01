namespace StreamTail.Options;

public sealed class ChannelPoolOptions
{
    public int MaxPoolSize { get; set; } = 500;
    public int MinPoolSize { get; set; } = 1;
    public TimeSpan IdleChannelTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan SweepInterval { get; set; } = TimeSpan.FromMinutes(1);
}
