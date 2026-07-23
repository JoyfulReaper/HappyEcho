/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using HappyEcho.Events;
using JoyfulReaperLib.JRNet;
using JoyfulReaperLib.MissionControl;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho;

public class EchoWorker(
    ILogger<EchoWorker> logger,
    IMissionControlClient missionControlClient,
    IOptions<HappyEchoOptions> options) : BackgroundService
{
    private static readonly TimeSpan TelemetryPublishTimeout =
        TimeSpan.FromSeconds(2);

    private TcpListener? _listener;
    private readonly ConcurrentDictionary<long, Task> _activeConnections = new();
    private volatile bool _stopRequested;
    private readonly SemaphoreSlim _connectionLimit = new(
        options.Value.MaxConcurrentConnections,
        options.Value.MaxConcurrentConnections
    );
    private long _nextConnectionId;
    private IPAddress? _localBoundAddress;
    public int BoundPort { get; private set; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _localBoundAddress = IPAddressUtils.ParseListenAddress(options.Value.ListenAddress);
        _listener = new TcpListener(_localBoundAddress, options.Value.Port);
        _listener.Start();
        BoundPort = ((IPEndPoint)_listener.LocalEndpoint).Port;
        var occurredAt = DateTimeOffset.UtcNow;

        logger.LogInformation("HappyEcho server started on {IPAddress}:{Port}", _localBoundAddress, options.Value.Port);

        await PublishServiceStartedTelemetryAsync(
            $"{_localBoundAddress}:{options.Value.Port}",
            occurredAt,
            stoppingToken);

        try
        {
            while (!_stopRequested && !stoppingToken.IsCancellationRequested)
            {
                TcpClient? client = null;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    client?.Dispose();
                    break;
                }
                catch (SocketException) when (stoppingToken.IsCancellationRequested || _stopRequested)
                {
                    client?.Dispose();
                    break;
                }

                if (!_connectionLimit.Wait(0))
                {
                    logger.LogInformation("[REJECTED] Server busy (All {Max} slots taken). Dropping immediate connection.", options.Value.MaxConcurrentConnections);
                    client?.Dispose();
                    continue;
                }

                long connectionId = Interlocked.Increment(ref _nextConnectionId);
                Task task = HandleClientAsync(connectionId, client, stoppingToken);
                _activeConnections[connectionId] = task;

                _ = task.ContinueWith(t =>
                {
                    _activeConnections.TryRemove(connectionId, out _);
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            }
        }
        finally
        {
            _listener.Stop();
            Task[] remaining = _activeConnections.Values.ToArray();
            if (remaining.Length > 0)
            {
                try
                {
                    await Task.WhenAll(remaining);
                }
                catch
                {
                    // Normal Shutdown
                }
            }
        }
    }

    private async Task HandleClientAsync(
        long connectionId,
        TcpClient client,
        CancellationToken stoppingToken)
    {
        Task? startedTelemetryTask = null;
        EchoSessionTelemetryResult? telemetry = null;

        using (client)
        {
            try
            {
                client.NoDelay = true;
                EndPoint? remote = client.Client.RemoteEndPoint;


                // Mitigate Loop Attacks (Block local / loopback sources)
                if (client.Client.RemoteEndPoint is IPEndPoint ipEndPoint && options.Value.BlockLoopbackConnections)
                {
                    if (IPAddress.IsLoopback(ipEndPoint.Address) || ipEndPoint.Address.Equals(_localBoundAddress))
                    {
                        logger.LogWarning("[SECURITY] Dropped loopback connection from {remote}", remote);
                        client.Close();
                        return;
                    }
                }

                bool isIgnoredTelemetrySource = IsIgnoredTelemetrySource(remote);
                string correlationId = Guid.NewGuid().ToString("N");

                if (!isIgnoredTelemetrySource)
                {
                    startedTelemetryTask = PublishStreamingStartedAsync(
                        remote?.ToString() ?? "unknown",
                        DateTimeOffset.UtcNow,
                        correlationId,
                        stoppingToken);
                }

                try
                {
                    logger.LogDebug("Received request: request from {Remote}.", client.Client.RemoteEndPoint);
                    await using NetworkStream stream = client.GetStream();
                    if (isIgnoredTelemetrySource)
                    {
                        logger.LogDebug(
                            "Skipping telemetry for monitoring connection from {Remote}.",
                            remote);
                    }

                    EchoProtocolResult result =
                        await EchoConnectionHandler.ProcessAsync(
                            stream,
                            remote,
                            options.Value,
                            logger,
                            stoppingToken);

                    if (!isIgnoredTelemetrySource)
                    {
                        telemetry = new EchoSessionTelemetryResult(
                            Remote: remote?.ToString() ?? "unknown",
                            BytesEchoed: result.BytesEchoed,
                            DurationMilliseconds: result.DurationMilliseconds,
                            Outcome: result.Outcome,
                            Succeeded: result.Succeeded,
                            OccurredAt: DateTimeOffset.UtcNow,
                            CorrelationId: correlationId);
                    }
                }
                catch (OperationCanceledException)
                {
                    logger.LogDebug(
                        "Connection {ConnectionId} from {Remote} timed out.",
                        connectionId,
                        remote);
                }
                catch (InvalidDataException exception)
                {
                    logger.LogInformation(
                        exception,
                        "Rejected malformed request on connection {ConnectionId} from {Remote}.",
                        connectionId,
                        remote);
                }
                catch (IOException exception)
                {
                    logger.LogDebug(
                        exception,
                        "Connection {ConnectionId} from {Remote} ended early.",
                        connectionId,
                        remote);
                }
                catch (SocketException exception)
                {
                    logger.LogDebug(
                        exception,
                        "Socket error on connection {ConnectionId} from {Remote}.",
                        connectionId,
                        remote);
                }
                catch (Exception exception)
                {
                    logger.LogError(
                        exception,
                        "Unhandled error on connection {ConnectionId} from {Remote}.",
                        connectionId,
                        remote);
                }
            }
            finally
            {
                _connectionLimit.Release();
            }
        }

        if (startedTelemetryTask is not null)
        {
            await ObserveStartedTelemetryAsync(
                startedTelemetryTask,
                stoppingToken);
        }

        if (telemetry is not null)
        {
            await PublishStreamingStoppedAsync(
                telemetry,
                stoppingToken);
        }
    }

    internal async Task ProcessEchoSessionAsync(
        Stream stream,
        EndPoint? remote,
        CancellationToken stoppingToken)
    {
        bool isIgnoredTelemetrySource = IsIgnoredTelemetrySource(remote);
        string correlationId = Guid.NewGuid().ToString("N");
        Task? startedTelemetryTask = isIgnoredTelemetrySource
            ? null
            : PublishStreamingStartedAsync(
                remote?.ToString() ?? "unknown",
                DateTimeOffset.UtcNow,
                correlationId,
                stoppingToken);

        if (isIgnoredTelemetrySource)
        {
            logger.LogDebug(
                "Skipping telemetry for monitoring connection from {Remote}.",
                remote);
        }

        EchoProtocolResult result;

        try
        {
            result = await EchoConnectionHandler.ProcessAsync(
                stream,
                remote,
                options.Value,
                logger,
                stoppingToken);
        }
        finally
        {
            await stream.DisposeAsync();
        }

        EchoSessionTelemetryResult? telemetry =
            isIgnoredTelemetrySource
                ? null
                : new EchoSessionTelemetryResult(
                    Remote: remote?.ToString() ?? "unknown",
                    BytesEchoed: result.BytesEchoed,
                    DurationMilliseconds: result.DurationMilliseconds,
                    Outcome: result.Outcome,
                    Succeeded: result.Succeeded,
                    OccurredAt: DateTimeOffset.UtcNow,
                    CorrelationId: correlationId);

        if (startedTelemetryTask is not null)
        {
            await ObserveStartedTelemetryAsync(
                startedTelemetryTask,
                stoppingToken);
        }

        if (telemetry is not null)
        {
            await PublishStreamingStoppedAsync(
                telemetry,
                stoppingToken);
        }
    }

    private bool IsIgnoredTelemetrySource(
        EndPoint? remote)
    {
        var remoteAddress = (remote as IPEndPoint)?
            .Address
            .MapToIPv4()
            .ToString();

        return
            !string.IsNullOrWhiteSpace(
                options.Value.TelemetryIgnoredRemoteAddress) &&
            string.Equals(
                remoteAddress,
                options.Value.TelemetryIgnoredRemoteAddress,
                StringComparison.OrdinalIgnoreCase);
    }

    private async Task PublishStreamingStartedAsync(
        string remote,
        DateTimeOffset occurredAt,
        string correlationId,
        CancellationToken cancellationToken)
    {
        using var timeout =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.StreamingStarted,
                payload: new StreamingStartedEvent(
                    remote,
                    options.Value.RequestTimeoutSeconds,
                    options.Value.MaxBytesPerConnection),
                payloadTypeInfo: HappyEchoJsonContext.Default.StreamingStartedEvent,
                occurredAt,
                correlationId,
                timeout.Token);

        if (!published)
        {
            logger.LogWarning(
                "Mission Control did not accept {EventType}",
                HappyEchoEventTypes.StreamingStarted);
        }
    }

    private async Task ObserveStartedTelemetryAsync(
        Task startedTelemetryTask,
        CancellationToken stoppingToken)
    {
        try
        {
            await startedTelemetryTask;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogDebug(
                "Streaming started telemetry publishing stopped during shutdown.");
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Timed out publishing Mission Control streaming started event.");
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish Mission Control streaming started event.");
        }
    }

    private async Task PublishStreamingStoppedAsync(
        EchoSessionTelemetryResult telemetry,
        CancellationToken cancellationToken)
    {
        using var timeout =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.StreamingStopped,
                payload: new StreamingStoppedEvent(
                    telemetry.Remote,
                    telemetry.BytesEchoed,
                    telemetry.DurationMilliseconds,
                    telemetry.Outcome,
                    telemetry.Succeeded),
                payloadTypeInfo: HappyEchoJsonContext.Default.StreamingStoppedEvent,
                telemetry.OccurredAt,
                telemetry.CorrelationId,
                timeout.Token);

            if (!published)
            {
                logger.LogWarning(
                    "Mission Control did not accept {EventType}",
                    HappyEchoEventTypes.StreamingStopped);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(
                "Streaming stopped telemetry publishing stopped during shutdown.");
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Timed out publishing Mission Control streaming stopped event.");
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish Mission Control streaming stopped event.");
        }
    }

    private async Task PublishServiceStartedTelemetryAsync(
        string endpoint,
        DateTimeOffset occurredAt,
        CancellationToken stoppingToken)
    {
        using var timeout =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeout.CancelAfter(TelemetryPublishTimeout);

        try
        {
            bool published = await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.ServiceStarted,
                payload: new EchoServiceStartedEvent(endpoint),
                payloadTypeInfo: HappyEchoJsonContext.Default.EchoServiceStartedEvent,
                occurredAt: occurredAt,
                correlationId: null,
                cancellationToken: timeout.Token);

            if (!published)
            {
                logger.LogWarning(
                    "Mission Control did not accept {EventType}",
                    HappyEchoEventTypes.ServiceStarted);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
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

    public static Task<long> EchoAsync(
        Stream stream,
        int RequestTimeoutSeconds,
        long maxBytesPerConnection,
        CancellationToken stoppingToken) =>
        EchoConnectionHandler.EchoAsync(
            stream,
            RequestTimeoutSeconds,
            maxBytesPerConnection,
            stoppingToken);

    internal static Task EchoAsync(
        Stream stream,
        int RequestTimeoutSeconds,
        long maxBytesPerConnection,
        CancellationToken stoppingToken,
        EchoSessionState state) =>
        EchoConnectionHandler.EchoAsync(
            stream,
            RequestTimeoutSeconds,
            maxBytesPerConnection,
            stoppingToken,
            state);

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("HappyEcho Server Stopping...");
        _stopRequested = true;
        _listener?.Stop();

        return base.StopAsync(cancellationToken);
    }

    private sealed record EchoSessionTelemetryResult(
        string Remote,
        long BytesEchoed,
        long DurationMilliseconds,
        string Outcome,
        bool Succeeded,
        DateTimeOffset OccurredAt,
        string CorrelationId);
}
