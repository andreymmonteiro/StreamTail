using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using StreamTail.Channels;
using StreamTail.Logging;
using StreamTail.Options;

namespace StreamTail.DI;

public static class StreamTailServiceCollectionExtension
{
    public static IServiceCollection AddStreamTail(this IServiceCollection services, ChannelPoolOptions? options = null)
    {
        services.AddSingleton<IChannelPool>(sp =>
        {
            var connection = sp.GetRequiredService<IConnection>();
            return new ChannelPool(connection, options);
        });

        services.AddScoped<IExceptionNotifier, ExceptionNotifier>();

        return services;
    }
}
