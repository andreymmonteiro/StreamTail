using StreamTail.Events;

namespace StreamTail.Publishing;

public interface IPublisherHandler<TEvent> where TEvent : IDomainEvent
{
    Task PublishAsync(TEvent @event, CancellationToken ct = default);
}
