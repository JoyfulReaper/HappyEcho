/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using JoyfulReaperLib.JRNet;
using JoyfulReaperLib.MissionControl;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho;

public class EchoWorker(
    ILogger<EchoWorker> logger,
    IMissionControlClient missionControlClient,
    IOptions<HappyEchoOptions> options) : BackgroundService
{
    private TcpListener? _listener;
    private readonly ConcurrentDictionary<long, Task> _activeConnections = new();
    private volatile bool _stopRequested;
    private readonly SemaphoreSlim _connectionLimit = new(
        options.Value.MaxConcurrentConnections,
        options.Value.MaxConcurrentConnections
    );
    private long _nextConnectionId;
    private IPAddress? _localBoundAddress;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _localBoundAddress = IPAddressUtils.ParseListenAddress(options.Value.ListenAddress);
        _listener = new TcpListener(_localBoundAddress, options.Value.Port);
        _listener.Start();

        logger.LogInformation("HappyEcho server started on {IPAddress}:{Port}", _localBoundAddress, options.Value.Port);

        try
        {
            TcpClient client;
            while (!_stopRequested && !stoppingToken.IsCancellationRequested)
            {
                try
                {
                    client = await _listener.AcceptTcpClientAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (SocketException) when (stoppingToken.IsCancellationRequested || _stopRequested)
                {
                    break;
                }

                if (!_connectionLimit.Wait(0))
                {
                    logger.LogInformation("[REJECTED] Server busy (All {Max} slots taken). Dropping immediate connection.", options.Value.MaxConcurrentConnections);
                    client.Dispose();
                    continue;
                }

                long connectionId = Interlocked.Increment(ref _nextConnectionId);
                Task task = HandleClientAsync(connectionId, client, stoppingToken);
                _activeConnections[connectionId] = task;

                _ = task.ContinueWith(t =>
                {
                    _activeConnections.TryRemove(connectionId, out _);
                    _connectionLimit.Release();
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
        using (client)
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


            try
            {
                logger.LogDebug("Received request: request from {Remote}.", client.Client.RemoteEndPoint);
                NetworkStream stream = client.GetStream();
                await ProcessEchoSessionAsync(stream, remote, stoppingToken);
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
    }

    internal async Task ProcessEchoSessionAsync(
        Stream stream,
        EndPoint? remote,
        CancellationToken stoppingToken)
    {
        string remoteString = remote?.ToString() ?? "unknown";
        bool isIgnoredTelemetrySource = IsIgnoredTelemetrySource(remote);
        Stopwatch stopwatch = Stopwatch.StartNew();
        var state = new EchoSessionState();

        if (isIgnoredTelemetrySource)
        {
            logger.LogDebug(
                "Skipping telemetry for monitoring connection from {Remote}.",
                remote);
        }

        string correlationId = Guid.NewGuid().ToString("N");
        DateTimeOffset startedOccurredAt = DateTimeOffset.UtcNow;
        Task? startedTelemetryTask = isIgnoredTelemetrySource
            ? null
            : PublishStreamingStartedAsync(
                remoteString,
                startedOccurredAt,
                correlationId,
                CancellationToken.None);

        string outcome = "failed";
        bool succeeded = false;

        try
        {
            await EchoAsync(
                stream,
                options.Value.RequestTimeoutSeconds,
                options.Value.MaxBytesPerConnection,
                stoppingToken,
                state);

            outcome = state.ByteLimitReached
                ? "byte-limit-reached"
                : "client-disconnected";
            succeeded = true;
        }
        catch (OperationCanceledException)
        when (stoppingToken.IsCancellationRequested)
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
                "Socket error during echo session from {Remote}.",
                remote);
        }
        catch (Exception exception)
        {
            outcome = "failed";

            logger.LogError(
                exception,
                "Unhandled error during echo session from {Remote}.",
                remote);
        }
        finally
        {
            stopwatch.Stop();

            try
            {
                await stream.DisposeAsync();
            }
            catch (Exception exception)
            {
                logger.LogDebug(
                    exception,
                    "Failed to dispose echo stream for {Remote}.",
                    remote);
            }
        }

        DateTimeOffset stoppedOccurredAt = DateTimeOffset.UtcNow;

        if (isIgnoredTelemetrySource)
        {
            return;
        }

        Debug.Assert(startedTelemetryTask is not null);
        await ObserveStartedTelemetryAsync(startedTelemetryTask);

        await PublishStreamingStoppedAsync(
            remoteString,
            state.BytesEchoed,
            stopwatch.ElapsedMilliseconds,
            outcome,
            succeeded,
            stoppedOccurredAt,
            correlationId,
            CancellationToken.None);
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
        await missionControlClient.TryPublishAsync(
            eventType: HappyEchoEventTypes.StreamingStarted,
            payload: new StreamingStartedEvent(
                remote,
                options.Value.RequestTimeoutSeconds,
                options.Value.MaxBytesPerConnection),
            occurredAt,
            correlationId,
            cancellationToken);
    }

    private async Task ObserveStartedTelemetryAsync(
        Task startedTelemetryTask)
    {
        try
        {
            await startedTelemetryTask;
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish Mission Control streaming started event.");
        }
    }

    private async Task PublishStreamingStoppedAsync(
        string remote,
        long bytesEchoed,
        long durationMilliseconds,
        string outcome,
        bool succeeded,
        DateTimeOffset occurredAt,
        string correlationId,
        CancellationToken cancellationToken)
    {
        try
        {
            await missionControlClient.TryPublishAsync(
                eventType: HappyEchoEventTypes.StreamingStopped,
                payload: new StreamingStoppedEvent(
                    remote,
                    bytesEchoed,
                    durationMilliseconds,
                    outcome,
                    succeeded),
                occurredAt,
                correlationId,
                cancellationToken);
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Failed to publish Mission Control streaming stopped event.");
        }
    }

    public static async Task<long> EchoAsync(
        Stream stream,
        int RequestTimeoutSeconds,
        long maxBytesPerConnection,
        CancellationToken stoppingToken)
    {
        var state = new EchoSessionState();

        await EchoAsync(
            stream,
            RequestTimeoutSeconds,
            maxBytesPerConnection,
            stoppingToken,
            state);

        return state.BytesEchoed;
    }

    internal static async Task EchoAsync(
        Stream stream,
        int RequestTimeoutSeconds,
        long maxBytesPerConnection,
        CancellationToken stoppingToken,
        EchoSessionState state)
    {
        const int BUFFER_SIZE = 4096;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(BUFFER_SIZE);

        // We dont want to keep echoing data forever so we set a timeout
        using var timeout =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(RequestTimeoutSeconds));

        try
        {
            while (state.BytesEchoed < maxBytesPerConnection)
            {
                long remaining =
                    maxBytesPerConnection - state.BytesEchoed;

                int readSize = (int)Math.Min(
                                BUFFER_SIZE,
                                remaining);

                int bytesRead = await stream.ReadAsync(
                    buffer.AsMemory(0, readSize),
                    timeout.Token);

                if (bytesRead == 0)
                {
                    // Client disconnected
                    break;
                }

                await stream.WriteAsync(buffer.AsMemory(0, bytesRead), timeout.Token);
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

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("HappyEcho Server Stopping...");
        _stopRequested = true;
        _listener?.Stop();

        return base.StopAsync(cancellationToken);
    }
}
