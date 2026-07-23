/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using HappyEcho.Events;
using JoyfulReaperLib.JRNet;
using JoyfulReaperLib.MissionControl;
using JoyfulReaperLib.TcpServer;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho;

/// <summary>
/// Processes Echo protocol connections and publishes connection telemetry.
/// </summary>
public sealed class EchoConnectionHandler(
    ILogger<EchoConnectionHandler> logger,
    IMissionControlClient missionControlClient,
    IOptions<HappyEchoOptions> options) : ITcpConnectionHandler
{
    private static readonly TimeSpan TelemetryPublishTimeout = TimeSpan.FromSeconds(2);

    private readonly IPAddress _configuredListenAddress = IPAddressUtils.ParseListenAddress(options.Value.ListenAddress);

    /// <inheritdoc />
    public async ValueTask HandleAsync(TcpConnectionContext context, CancellationToken cancellationToken)
    {
        EndPoint? remote = context.RemoteEndPoint;

        if (ShouldBlockConnection(remote))
        {
            logger.LogWarning(
                "[SECURITY] Dropped loopback connection from {Remote}",
                remote);

            return;
        }

        if (IsIgnoredTelemetrySource(remote))
        {
            logger.LogDebug(
                "Skipping telemetry for monitoring connection from {Remote}.",
                remote);

            _ = await ProcessAsync(
                context.Stream,
                remote,
                options.Value,
                logger,
                cancellationToken);

            return;
        }

        string remoteString = remote?.ToString() ?? "unknown";
        string correlationId = Guid.NewGuid().ToString("N");

        Task startedTelemetryTask = PublishStreamingStartedAsync(
            remoteString,
            DateTimeOffset.UtcNow,
            correlationId,
            cancellationToken);

        EchoProtocolResult protocolResult = await ProcessAsync(
            context.Stream,
            remote,
            options.Value,
            logger,
            cancellationToken);

        var telemetry = new EchoSessionTelemetryResult(
            Remote: remoteString,
            BytesEchoed: protocolResult.BytesEchoed,
            DurationMilliseconds: protocolResult.DurationMilliseconds,
            Outcome: protocolResult.Outcome,
            Succeeded: protocolResult.Succeeded,
            OccurredAt: DateTimeOffset.UtcNow,
            CorrelationId: correlationId);

        context.RegisterAfterClose(afterCloseToken =>
            CompleteTelemetryAsync(
                context.ConnectionId,
                startedTelemetryTask,
                telemetry,
                afterCloseToken));
    }

    private bool ShouldBlockConnection(EndPoint? remote)
    {
        if (!options.Value.BlockLoopbackConnections || remote is not IPEndPoint remoteEndPoint)
        {
            return false;
        }

        return IPAddress.IsLoopback(remoteEndPoint.Address) ||
            remoteEndPoint.Address.Equals(_configuredListenAddress);
    }

    private bool IsIgnoredTelemetrySource(EndPoint? remote)
    {
        string? remoteAddress = (remote as IPEndPoint)?.Address.MapToIPv4().ToString();

        return !string.IsNullOrWhiteSpace(options.Value.TelemetryIgnoredRemoteAddress) &&
            string.Equals(remoteAddress, options.Value.TelemetryIgnoredRemoteAddress, StringComparison.OrdinalIgnoreCase);
    }

    private async ValueTask CompleteTelemetryAsync(
        long connectionId,
        Task startedTelemetryTask,
        EchoSessionTelemetryResult telemetry,
        CancellationToken cancellationToken)
    {
        try
        {
            await startedTelemetryTask;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(
                "Streaming-started telemetry for connection {ConnectionId} was cancelled during shutdown.",
                connectionId);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Streaming-started telemetry for connection {ConnectionId} timed out.",
                connectionId);
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish streaming-started telemetry for connection {ConnectionId}.",
                connectionId);
        }

        await PublishStreamingStoppedAsync(
            connectionId,
            telemetry,
            cancellationToken);
    }

    private async Task PublishStreamingStartedAsync(
        string remote,
        DateTimeOffset occurredAt,
        string correlationId,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        timeout.CancelAfter(TelemetryPublishTimeout);

        bool published = await missionControlClient.TryPublishAsync(
            eventType: HappyEchoEventTypes.StreamingStarted,
            payload: new StreamingStartedEvent(
                Remote: remote,
                RequestTimeoutSeconds: options.Value.RequestTimeoutSeconds,
                MaxBytesPerConnection: options.Value.MaxBytesPerConnection),
            payloadTypeInfo: HappyEchoJsonContext.Default.StreamingStartedEvent,
            occurredAt: occurredAt,
            correlationId: correlationId,
            cancellationToken: timeout.Token);

        if (!published)
        {
            logger.LogWarning(
                "Mission Control did not accept {EventType}.",
                HappyEchoEventTypes.StreamingStarted);
        }
    }

    private async ValueTask PublishStreamingStoppedAsync(
        long connectionId,
        EchoSessionTelemetryResult telemetry,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.StreamingStopped,
                payload: new StreamingStoppedEvent(
                    Remote: telemetry.Remote,
                    BytesEchoed: telemetry.BytesEchoed,
                    DurationMilliseconds: telemetry.DurationMilliseconds,
                    Outcome: telemetry.Outcome,
                    Succeeded: telemetry.Succeeded),
                payloadTypeInfo: HappyEchoJsonContext.Default.StreamingStoppedEvent,
                occurredAt: telemetry.OccurredAt,
                correlationId: telemetry.CorrelationId,
                cancellationToken: timeout.Token);

            if (!published)
            {
                logger.LogWarning(
                    "Mission Control did not accept {EventType} for connection {ConnectionId}.",
                    HappyEchoEventTypes.StreamingStopped,
                    connectionId);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(
                "Streaming-stopped telemetry for connection {ConnectionId} was cancelled during shutdown.",
                connectionId);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Streaming-stopped telemetry for connection {ConnectionId} timed out.",
                connectionId);
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish streaming-stopped telemetry for connection {ConnectionId}.",
                connectionId);
        }
    }

    internal static async ValueTask<EchoProtocolResult> ProcessAsync(
        Stream stream,
        EndPoint? remote,
        HappyEchoOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        var state = new EchoSessionState();

        string outcome = "failed";
        bool succeeded = false;

        try
        {
            await EchoAsync(
                stream,
                options.RequestTimeoutSeconds,
                options.MaxBytesPerConnection,
                cancellationToken,
                state);

            outcome = state.ByteLimitReached
                ? "byte-limit-reached"
                : "client-disconnected";

            succeeded = true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            outcome = "server-shutdown";

            logger.LogDebug(
                "Echo session from {Remote} was cancelled during shutdown.",
                remote);
        }
        catch (OperationCanceledException)
        {
            outcome = "timeout";

            logger.LogDebug(
                "Echo session from {Remote} timed out.",
                remote);
        }
        catch (IOException exception)
        {
            outcome = "io-error";

            logger.LogDebug(
                exception,
                "Echo session from {Remote} ended early.",
                remote);
        }
        catch (SocketException exception)
        {
            outcome = "socket-error";

            logger.LogDebug(
                exception,
                "Socket error during Echo session from {Remote}.",
                remote);
        }
        catch (Exception exception)
        {
            outcome = "failed";

            logger.LogError(
                exception,
                "Unhandled error during Echo session from {Remote}.",
                remote);
        }
        finally
        {
            stopwatch.Stop();
        }

        return new EchoProtocolResult(
            BytesEchoed: state.BytesEchoed,
            DurationMilliseconds: stopwatch.ElapsedMilliseconds,
            Outcome: outcome,
            Succeeded: succeeded);
    }

    internal static async Task<long> EchoAsync(
        Stream stream,
        int requestTimeoutSeconds,
        long maxBytesPerConnection,
        CancellationToken cancellationToken)
    {
        var state = new EchoSessionState();

        await EchoAsync(
            stream,
            requestTimeoutSeconds,
            maxBytesPerConnection,
            cancellationToken,
            state);

        return state.BytesEchoed;
    }

    internal static async Task EchoAsync(
        Stream stream,
        int requestTimeoutSeconds,
        long maxBytesPerConnection,
        CancellationToken cancellationToken,
        EchoSessionState state)
    {
        const int BufferSize = 4096;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        timeout.CancelAfter(TimeSpan.FromSeconds(requestTimeoutSeconds));

        try
        {
            while (state.BytesEchoed < maxBytesPerConnection)
            {
                long remaining = maxBytesPerConnection - state.BytesEchoed;

                int readSize = (int)Math.Min(BufferSize, remaining);

                int bytesRead = await stream.ReadAsync(
                    buffer.AsMemory(0, readSize),
                    timeout.Token);

                if (bytesRead == 0)
                {
                    break;
                }

                await stream.WriteAsync(
                    buffer.AsMemory(0, bytesRead),
                    timeout.Token);

                await stream.FlushAsync(timeout.Token);

                state.BytesEchoed += bytesRead;
            }

            state.ByteLimitReached = state.BytesEchoed >= maxBytesPerConnection;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}

internal sealed record EchoProtocolResult(
    long BytesEchoed,
    long DurationMilliseconds,
    string Outcome,
    bool Succeeded);

internal sealed record EchoSessionTelemetryResult(
    string Remote,
    long BytesEchoed,
    long DurationMilliseconds,
    string Outcome,
    bool Succeeded,
    DateTimeOffset OccurredAt,
    string CorrelationId);
