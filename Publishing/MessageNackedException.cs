namespace StreamTail.Publishing;

public sealed class MessageNackedException : Exception
{
    public MessageNackedException()
        : base("The RabbitMQ broker sent a Nack for the published message.") { }

    public MessageNackedException(string message) : base(message) { }
}
