# StreamTail

StreamTail is a small, high-performance producer/consumer abstraction for RabbitMQ targeting modern .NET (net10.0).

It provides:
- A channel pooling implementation to reduce channel creation overhead
- Optional connection pooling for multi-connection scenarios
- A simple publisher abstraction with configurable options
- An exception notifier that can send failed messages to a DLQ

This README explains what the library does, how to set it up, and shows common usage patterns.

Prerequisites
-------------
- .NET 10 SDK
- A running RabbitMQ server (for integration/runtime scenarios)

Install
-------

dotnet add package StreamTail --version 2.0.0

Basic concepts
--------------
- IChannelPool: pooled channels (IChannel) for publishing/other channel operations. Rent channels with RentAsync and return automatically when the lease is disposed.
- IConnectionProvider / ConnectionPool: optional connection pooling when you need multiple RabbitMQ connections.
- IPublisherHandler<TEvent>: a simple publisher for domain events (TEvent : IDomainEvent).
- IExceptionNotifier: helper to notify failures by publishing a message to a configured DLQ (Dead Letter Queue).

Dependency injection
--------------------
StreamTail integrates with Microsoft.Extensions.DependencyInjection. There are two primary registration options:

- Register using a single existing IConnection (simple case):

services.AddStreamTail(channelOptions);

This expects an IConnection already registered in the container.

- Register with connection pooling (v2.0) — the library will manage connections for you:

services.AddStreamTailWithConnectionPooling(channelOptions, connectionOptions);

Examples
--------
1) Registering a single connection and a publisher

// configure RabbitMQ connection factory and open a connection
var factory = new ConnectionFactory { HostName = "localhost" };
var connection = await factory.CreateConnectionAsync();
services.AddSingleton(connection as IConnection);

// register StreamTail and a publisher for MyEvent
services.AddStreamTail();
services.AddPublisher<MyEvent>(opts =>
{
	opts.Exchange = "my-exchange";
	opts.RoutingKey = "my.routing.key";
	opts.Persistent = true;
});

// Using the publisher from an injected service
public class MyService
{
	private readonly IPublisherHandler<MyEvent> _publisher;

	public MyService(IPublisherHandler<MyEvent> publisher) => _publisher = publisher;

	public async Task DoWorkAsync()
	{
		var evt = new MyEvent { /* ... */ };
		await _publisher.PublishAsync(evt);
	}
}

2) Using connection pooling and channel options

services.AddSingleton<IConnectionFactory>(sp => new ConnectionFactory { HostName = "localhost" });

var channelOptions = new StreamTail.Options.ChannelPoolOptions
{
	MaxPoolSize = 200,
	MinPoolSize = 2,
	IdleChannelTimeout = TimeSpan.FromMinutes(2),
	SweepInterval = TimeSpan.FromMinutes(1)
};

var connectionOptions = new StreamTail.Options.ConnectionPoolOptions
{
	MaxConnections = 5,
	MinConnections = 1
};

services.AddStreamTailWithConnectionPooling(channelOptions, connectionOptions);

3) Manually renting a channel (advanced scenarios)

// Inject IChannelPool and rent a channel directly when you need low-level operations
public class LowLevelPublisher
{
	private readonly StreamTail.Channels.IChannelPool _pool;

	public LowLevelPublisher(StreamTail.Channels.IChannelPool pool) => _pool = pool;

	public async Task PublishRawAsync(byte[] body, string exchange, string routingKey, CancellationToken ct = default)
	{
		await using var lease = await _pool.RentAsync(ct);
		var channel = lease.Channel;
		var props = new RabbitMQ.Client.BasicProperties { Persistent = true };
		await channel.BasicPublishAsync(exchange: exchange, routingKey: routingKey, mandatory: false, basicProperties: props, body: body, cancellationToken: ct);
	}
}

4) Using the exception notifier to send failed messages to a DLQ

// IExceptionNotifier is registered by the DI helpers
public class ConsumerWorker
{
	private readonly StreamTail.Logging.IExceptionNotifier _notifier;

	public ConsumerWorker(StreamTail.Logging.IExceptionNotifier notifier) => _notifier = notifier;

	public async Task HandleFailureAsync(Exception ex, string message)
	{
		// Provide exchange and dlqName where failed payloads should be published
		await _notifier.Notify(ex, exchange: "", dlqName: "my-dlq", message: message, cancellationToken: CancellationToken.None);
	}
}

Configuration options
---------------------
- ChannelPoolOptions: MaxPoolSize, MinPoolSize, IdleChannelTimeout, SweepInterval.
- ConnectionPoolOptions: MaxConnections, MinConnections, IdleConnectionTimeout, retry behavior (see ConnectionPoolOptions in code).
- PublisherOptions: Exchange, RoutingKey, Persistent, UseConfirms, ConfirmTimeout, and custom Serializer.

Design notes and behavior
-------------------------
- Channels are pooled and reused to avoid the cost of channel creation on every publish.
- When RentAsync is called, a semaphore is used to apply backpressure when the pool is exhausted.
- The pool has a background sweeper that disposes idle channels above the minimum pool size after the configured idle timeout.
- ExceptionNotifier publishes failure information to a user-specified DLQ exchange/routing key.

Contributing and tests
----------------------
The repository contains unit and integration tests (see StreamTail.UnitTests and StreamTail.IntegrationTests). To run tests:

dotnet test

Notes
-----
- The library targets net10.0. Adjust your application's target framework accordingly.
- This README focuses on publishing and channel pooling; consumers (message handlers) are out of scope and should use the standard RabbitMQ consumer patterns.

Questions
---------
If something in the examples does not match your environment, consult the code in the DI folder and the options classes for precise configuration details.

