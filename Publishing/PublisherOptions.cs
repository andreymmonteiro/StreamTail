namespace StreamTail.Publishing;

public sealed class PublisherOptions
{
    public string Exchange { get; set; } = string.Empty;
    public string RoutingKey { get; set; } = string.Empty;
    public bool Persistent { get; set; } = true;
    public bool UseConfirms { get; set; } = false;
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);
    // Override default JSON serialization. Receives the event as object; return the wire bytes.
    public Func<object, byte[]>? Serializer { get; set; }
}
