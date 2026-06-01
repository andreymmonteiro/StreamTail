namespace StreamTail.Logging;

public interface IExceptionNotifier
{
    Task Notify(Exception exception, string exchange, string dlqName, string message, CancellationToken cancellationToken);
}