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
using System.Text.Json.Serialization.Metadata;

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

        using UdpClient udp = CreateUdpClient(
            listenAddress,
            port,
            value.DualMode);
        string listenEndpoint = udp.Client.LocalEndPoint!.ToString()!;
        Stopwatch stopwatch = Stopwatch.StartNew();
        long datagramsReceived = 0;
        long datagramsEchoed = 0;
        long datagramsDropped = 0;
        long bytesEchoed = 0;

        TryLog(() =>
            logger.LogInformation(
                "HappyEcho UDP listener started on {Endpoint} (dual mode: {DualMode})",
                udp.Client.LocalEndPoint,
                value.DualMode));

        await PublishTelemetrySafelyAsync(
            HappyEchoEventTypes.UdpStarted,
            new UdpEchoStartedEvent(
                listenEndpoint,
                maxDatagramBytes,
                value.BlockLoopbackConnections),
            HappyEchoJsonContext.Default.UdpEchoStartedEvent,
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
                    await PublishTelemetrySafelyAsync(
                        HappyEchoEventTypes.UdpDatagramDropped,
                        new UdpDatagramDroppedEvent(
                            received.RemoteEndPoint.ToString(),
                            received.Buffer.Length,
                            "loopback-blocked"),
                        HappyEchoJsonContext.Default.UdpDatagramDroppedEvent,
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
                    await PublishTelemetrySafelyAsync(
                        HappyEchoEventTypes.UdpDatagramDropped,
                        new UdpDatagramDroppedEvent(
                            received.RemoteEndPoint.ToString(),
                            received.Buffer.Length,
                            "oversized"),
                        HappyEchoJsonContext.Default.UdpDatagramDroppedEvent,
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
                    await PublishTelemetrySafelyAsync(
                        HappyEchoEventTypes.UdpDatagramEchoed,
                        new UdpDatagramEchoedEvent(
                            received.RemoteEndPoint.ToString(),
                            received.Buffer.Length),
                        HappyEchoJsonContext.Default.UdpDatagramEchoedEvent,
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
                    await PublishTelemetrySafelyAsync(
                        HappyEchoEventTypes.UdpDatagramDropped,
                        new UdpDatagramDroppedEvent(
                            received.RemoteEndPoint.ToString(),
                            received.Buffer.Length,
                            "send-error"),
                        HappyEchoJsonContext.Default.UdpDatagramDroppedEvent,
                        stoppingToken);
                }
            }
        }
        finally
        {
            stopwatch.Stop();
            TryLog(() =>
                logger.LogInformation("HappyEcho UDP listener stopped."));
            await PublishTelemetrySafelyAsync(
                HappyEchoEventTypes.UdpStopped,
                new UdpEchoStoppedEvent(
                    listenEndpoint,
                    datagramsReceived,
                    datagramsEchoed,
                    datagramsDropped,
                    bytesEchoed,
                    stopwatch.ElapsedMilliseconds),
                HappyEchoJsonContext.Default.UdpEchoStoppedEvent,
                CancellationToken.None);
        }
    }

    private async Task PublishTelemetrySafelyAsync<TPayload>(
        string eventType,
        TPayload payload,
        JsonTypeInfo<TPayload> payloadTypeInfo,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: eventType,
                payload: payload,
                payloadTypeInfo: payloadTypeInfo,
                occurredAt: DateTimeOffset.UtcNow,
                cancellationToken: timeout.Token);

            if (!published)
            {
                TryLog(() =>
                    logger.LogWarning(
                        "Mission Control did not accept {EventType}.",
                        eventType));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            TryLog(() =>
                logger.LogDebug(
                    "Telemetry publishing for {EventType} stopped during shutdown.",
                    eventType));
        }
        catch (OperationCanceledException)
        {
            TryLog(() =>
                logger.LogWarning(
                    "Telemetry publishing for {EventType} timed out.",
                    eventType));
        }
        catch (Exception exception)
        {
            TryLog(() =>
                logger.LogWarning(
                    exception,
                    "Failed to publish Mission Control event {EventType}.",
                    eventType));
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

    private static UdpClient CreateUdpClient(
        IPAddress address,
        int port,
        bool dualMode)
    {
        if (dualMode &&
            !address.Equals(IPAddress.IPv6Any))
        {
            throw new InvalidOperationException(
                "UDP dual mode requires the UDP listen address to be the IPv6 wildcard address '::'.");
        }

        var udp = new UdpClient(address.AddressFamily);

        if (address.AddressFamily == AddressFamily.InterNetworkV6)
        {
            udp.Client.DualMode = dualMode;
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
