/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using HappyEcho.Events;
using JoyfulReaperLib.JRNet;
using JoyfulReaperLib.MissionControl;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho;

public sealed class UdpEchoService(
    ILogger<UdpEchoService> logger,
    IMissionControlClient missionControlClient,
    IOptions<HappyEchoOptions> options)
    : BackgroundService
{
    private const int MaximumUdpPayloadBytes = 65_507;
    private static readonly TimeSpan TelemetryPublishTimeout = TimeSpan.FromSeconds(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        HappyEchoOptions value = options.Value;

        if (!value.UdpEnabled)
        {
            TryLog(() =>
                logger.LogInformation("HappyEcho UDP listener disabled."));
            return;
        }

        IPAddress listenAddress = IPAddressUtils.ParseListenAddress(
            string.IsNullOrWhiteSpace(value.UdpListenAddress)
                ? value.ListenAddress
                : value.UdpListenAddress);

        int port = value.UdpPort ?? value.Port;
        int maxDatagramBytes = Math.Clamp(
            value.MaxUdpDatagramBytes,
            1,
            MaximumUdpPayloadBytes);

        using UdpClient udp = CreateUdpClient(listenAddress, port);
        string listenEndpoint = udp.Client.LocalEndPoint!.ToString()!;
        Stopwatch stopwatch = Stopwatch.StartNew();
        long datagramsReceived = 0;
        long datagramsEchoed = 0;
        long datagramsDropped = 0;
        long bytesEchoed = 0;

        TryLog(() =>
            logger.LogInformation(
                "HappyEcho UDP listener started on {Endpoint}",
                udp.Client.LocalEndPoint));

        await PublishStartedAsync(
            listenEndpoint,
            maxDatagramBytes,
            value.BlockLoopbackConnections,
            stoppingToken);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                UdpReceiveResult received;

                try
                {
                    received = await udp.ReceiveAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (SocketException exception)
                {
                    TryLog(() =>
                        logger.LogWarning(
                            exception,
                            "Socket error while receiving UDP Echo datagram."));

                    continue;
                }
                catch (ObjectDisposedException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                datagramsReceived++;

                if (ShouldBlockDatagram(received.RemoteEndPoint, listenAddress, value))
                {
                    datagramsDropped++;
                    TryLog(() =>
                        logger.LogWarning(
                            "[SECURITY] Dropped UDP loopback datagram from {Remote}",
                            received.RemoteEndPoint));
                    await PublishDroppedAsync(
                        received.RemoteEndPoint.ToString(),
                        received.Buffer.Length,
                        "loopback-blocked",
                        stoppingToken);

                    continue;
                }

                if (received.Buffer.Length > maxDatagramBytes)
                {
                    datagramsDropped++;
                    TryLog(() =>
                        logger.LogWarning(
                            "Dropped oversized UDP Echo datagram from {Remote}: {Bytes} bytes.",
                            received.RemoteEndPoint,
                            received.Buffer.Length));
                    await PublishDroppedAsync(
                        received.RemoteEndPoint.ToString(),
                        received.Buffer.Length,
                        "oversized",
                        stoppingToken);

                    continue;
                }

                try
                {
                    await udp.SendAsync(
                        received.Buffer,
                        received.Buffer.Length,
                        received.RemoteEndPoint);

                    datagramsEchoed++;
                    bytesEchoed += received.Buffer.Length;
                    TryLog(() =>
                        logger.LogDebug(
                            "Echoed UDP datagram for {Remote}: {Bytes} bytes.",
                            received.RemoteEndPoint,
                            received.Buffer.Length));
                    await PublishEchoedAsync(
                        received.RemoteEndPoint.ToString(),
                        received.Buffer.Length,
                        stoppingToken);
                }
                catch (SocketException exception)
                {
                    datagramsDropped++;
                    TryLog(() =>
                        logger.LogWarning(
                            exception,
                            "Socket error while sending UDP Echo datagram to {Remote}.",
                            received.RemoteEndPoint));
                    await PublishDroppedAsync(
                        received.RemoteEndPoint.ToString(),
                        received.Buffer.Length,
                        "send-error",
                        stoppingToken);
                }
            }
        }
        finally
        {
            stopwatch.Stop();
            TryLog(() =>
                logger.LogInformation("HappyEcho UDP listener stopped."));
            await PublishStoppedAsync(
                listenEndpoint,
                datagramsReceived,
                datagramsEchoed,
                datagramsDropped,
                bytesEchoed,
                stopwatch.ElapsedMilliseconds);
        }
    }

    private async Task PublishStartedAsync(
        string listenEndpoint,
        int maxDatagramBytes,
        bool blockLoopbackConnections,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.UdpStarted,
                payload: new UdpEchoStartedEvent(
                    listenEndpoint,
                    maxDatagramBytes,
                    blockLoopbackConnections),
                payloadTypeInfo: HappyEchoJsonContext.Default.UdpEchoStartedEvent,
                occurredAt: DateTimeOffset.UtcNow,
                cancellationToken: timeout.Token);

            if (!published)
            {
                TryLog(() =>
                    logger.LogWarning(
                        "Mission Control did not accept {EventType}.",
                        HappyEchoEventTypes.UdpStarted));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            TryLog(() =>
                logger.LogDebug(
                    "UDP-started telemetry publishing stopped during shutdown."));
        }
        catch (OperationCanceledException)
        {
            TryLog(() =>
                logger.LogWarning("UDP-started telemetry publishing timed out."));
        }
        catch (Exception exception)
        {
            TryLog(() =>
                logger.LogWarning(
                    exception,
                    "Failed to publish UDP-started telemetry."));
        }
    }

    private async Task PublishEchoedAsync(
        string remote,
        int bytesEchoed,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.UdpDatagramEchoed,
                payload: new UdpDatagramEchoedEvent(remote, bytesEchoed),
                payloadTypeInfo: HappyEchoJsonContext.Default.UdpDatagramEchoedEvent,
                occurredAt: DateTimeOffset.UtcNow,
                cancellationToken: timeout.Token);

            if (!published)
            {
                TryLog(() =>
                    logger.LogWarning(
                        "Mission Control did not accept {EventType}.",
                        HappyEchoEventTypes.UdpDatagramEchoed));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            TryLog(() =>
                logger.LogDebug(
                    "UDP-datagram-echoed telemetry publishing stopped during shutdown."));
        }
        catch (OperationCanceledException)
        {
            TryLog(() =>
                logger.LogWarning(
                    "UDP-datagram-echoed telemetry publishing timed out."));
        }
        catch (Exception exception)
        {
            TryLog(() =>
                logger.LogWarning(
                    exception,
                    "Failed to publish UDP-datagram-echoed telemetry."));
        }
    }

    private async Task PublishDroppedAsync(
        string remote,
        int bytesReceived,
        string reason,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.UdpDatagramDropped,
                payload: new UdpDatagramDroppedEvent(remote, bytesReceived, reason),
                payloadTypeInfo: HappyEchoJsonContext.Default.UdpDatagramDroppedEvent,
                occurredAt: DateTimeOffset.UtcNow,
                cancellationToken: timeout.Token);

            if (!published)
            {
                TryLog(() =>
                    logger.LogWarning(
                        "Mission Control did not accept {EventType}.",
                        HappyEchoEventTypes.UdpDatagramDropped));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            TryLog(() =>
                logger.LogDebug(
                    "UDP-datagram-dropped telemetry publishing stopped during shutdown."));
        }
        catch (OperationCanceledException)
        {
            TryLog(() =>
                logger.LogWarning(
                    "UDP-datagram-dropped telemetry publishing timed out."));
        }
        catch (Exception exception)
        {
            TryLog(() =>
                logger.LogWarning(
                    exception,
                    "Failed to publish UDP-datagram-dropped telemetry."));
        }
    }

    private async Task PublishStoppedAsync(
        string listenEndpoint,
        long datagramsReceived,
        long datagramsEchoed,
        long datagramsDropped,
        long bytesEchoed,
        long durationMilliseconds)
    {
        using var timeout = new CancellationTokenSource(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.UdpStopped,
                payload: new UdpEchoStoppedEvent(
                    listenEndpoint,
                    datagramsReceived,
                    datagramsEchoed,
                    datagramsDropped,
                    bytesEchoed,
                    durationMilliseconds),
                payloadTypeInfo: HappyEchoJsonContext.Default.UdpEchoStoppedEvent,
                occurredAt: DateTimeOffset.UtcNow,
                cancellationToken: timeout.Token);

            if (!published)
            {
                TryLog(() =>
                    logger.LogWarning(
                        "Mission Control did not accept {EventType}.",
                        HappyEchoEventTypes.UdpStopped));
            }
        }
        catch (OperationCanceledException)
        {
            TryLog(() =>
                logger.LogWarning("UDP-stopped telemetry publishing timed out."));
        }
        catch (Exception exception)
        {
            TryLog(() =>
                logger.LogWarning(
                    exception,
                    "Failed to publish UDP-stopped telemetry."));
        }
    }

    private static void TryLog(Action log)
    {
        try
        {
            log();
        }
        catch
        {
            // Logging must never interrupt UDP Echo or its telemetry safeguards.
        }
    }

    private static UdpClient CreateUdpClient(IPAddress address, int port)
    {
        var udp = new UdpClient(address.AddressFamily);

        if (address.AddressFamily == AddressFamily.InterNetworkV6 &&
            address.Equals(IPAddress.IPv6Any))
        {
            udp.Client.DualMode = true;
        }

        udp.Client.Bind(new IPEndPoint(address, port));

        return udp;
    }

    private static bool ShouldBlockDatagram(
        IPEndPoint remote,
        IPAddress configuredListenAddress,
        HappyEchoOptions options)
    {
        if (!options.BlockLoopbackConnections)
        {
            return false;
        }

        return IPAddress.IsLoopback(remote.Address) ||
            remote.Address.Equals(configuredListenAddress);
    }
}
