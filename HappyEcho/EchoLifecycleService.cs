/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using HappyEcho.Events;
using JoyfulReaperLib.JRNet;
using JoyfulReaperLib.MissionControl;
using Microsoft.Extensions.Options;

namespace HappyEcho;

/// <summary>
/// Handles application-level lifecycle telemetry for HappyEcho.
/// </summary>
public sealed class EchoLifecycleService(
    ILogger<EchoLifecycleService> logger,
    IMissionControlClient missionControlClient,
    IOptions<HappyEchoOptions> options) : IHostedLifecycleService
{
    private static readonly TimeSpan TelemetryPublishTimeout = TimeSpan.FromSeconds(2);

    public Task StartingAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    public async Task StartedAsync(CancellationToken cancellationToken)
    {
        var listenAddress = IPAddressUtils.ParseListenAddress(options.Value.ListenAddress);

        logger.LogInformation(
            "HappyEcho server started on {IPAddress}:{Port}",
            listenAddress,
            options.Value.Port);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.ServiceStarted,
                payload: new EchoServiceStartedEvent(
                    $"{listenAddress}:{options.Value.Port}"),
                payloadTypeInfo: HappyEchoJsonContext.Default.EchoServiceStartedEvent,
                occurredAt: DateTimeOffset.UtcNow,
                correlationId: null,
                cancellationToken: timeout.Token);

            if (!published)
            {
                logger.LogWarning(
                    "Mission Control did not accept {EventType}",
                    HappyEchoEventTypes.ServiceStarted);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(
                "Service-started telemetry publishing stopped during shutdown.");
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Timed out publishing Mission Control event for Echo Service Started.");
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish Mission Control event for Echo Service Started");
        }
    }

    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("HappyEcho Server Stopping...");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) =>
        Task.CompletedTask;
}