using Microsoft.Extensions.DependencyInjection;
using StreamTail.Channels;
using StreamTail.Logging;

namespace StreamTail.DI;

public static class StreamTailServiceCollectionExtension
{
    public static IServiceCollection AddStreamTail(this IServiceCollection services)
    {
        services.AddSingleton<IChannelPool, ChannelPool>();

        services.AddScoped<IExceptionNotifier, ExceptionNotifier>();

        return services;
    }
}
