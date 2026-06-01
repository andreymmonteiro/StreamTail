using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using StreamTail.Channels;
using StreamTail.Connections;
using StreamTail.Logging;
using StreamTail.Options;

namespace StreamTail.DI;

public static class StreamTailServiceCollectionExtension
{
    /// <summary>
    /// Registers StreamTail with a single pre-existing IConnection (v1.x behavior).
    /// </summary>
    public static IServiceCollection AddStreamTail(
        this IServiceCollection services,
        ChannelPoolOptions? options = null)
    {
        services.AddSingleton<IChannelPool>(sp =>
        {
            var connection = sp.GetRequiredService<IConnection>();
            return new ChannelPool(connection, options);
        });

        services.AddScoped<IExceptionNotifier, ExceptionNotifier>();

        return services;
    }

    /// <summary>
    /// Registers StreamTail with a connection pool (v2.0).
    /// </summary>
    public static IServiceCollection AddStreamTailWithConnectionPooling(
        this IServiceCollection services,
        ChannelPoolOptions? channelOptions = null,
        ConnectionPoolOptions? connectionOptions = null)
    {
        services.AddSingleton<IConnectionProvider>(sp => 
        { 
            var factory = sp.GetRequiredService<IConnectionFactory>();
            return new ConnectionPool(factory, connectionOptions);  
        });

        services.AddSingleton<IChannelPool>(sp =>
        {
            var provider = sp.GetRequiredService<IConnectionProvider>();
            return new ConnectionAwareChannelPool(provider, channelOptions);
        });

        services.AddScoped<IExceptionNotifier, ExceptionNotifier>();

        return services;
    }
}
