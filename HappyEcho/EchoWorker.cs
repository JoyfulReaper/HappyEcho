/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using JoyfulReaperLib.JRNet;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace HappyEcho;

public class EchoWorker(
    ILogger<EchoWorker> logger,
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
            while (!stoppingToken.IsCancellationRequested)
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
                    logger.LogWarning("[REJECTED] Server busy (All {Max} slots taken). Dropping immediate connection.", options.Value.MaxConcurrentConnections);
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
            if (client.Client.RemoteEndPoint is IPEndPoint ipEndPoint)
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
                await using NetworkStream stream = client.GetStream();
                await EchoAsync(stream, options.Value.RequestTimeoutSeconds, stoppingToken);

                logger.LogDebug("Received request: request from {Remote}.", client.Client.RemoteEndPoint);
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning(
                    "Connection {ConnectionId} from {Remote} timed out.",
                    connectionId,
                    remote);
            }
            catch (InvalidDataException exception)
            {
                logger.LogWarning(
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

    public static async Task EchoAsync(
        Stream stream,
        int RequestTimeoutSeconds,
        CancellationToken stoppingToken)
    {
        const int BUFFER_SIZE = 4096;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(BUFFER_SIZE);

        // We dont want to keep eching data forever so we set a timeout
        using var timeout =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(RequestTimeoutSeconds));

        try
        {
            while (true)
            {
                int bytesRead = await stream.ReadAsync(
                    buffer.AsMemory(0, BUFFER_SIZE),
                    timeout.Token);

                if (bytesRead == 0)
                {
                    // Client disconnected
                    return;
                }

                await stream.WriteAsync(buffer.AsMemory(0, bytesRead), stoppingToken);
                await stream.FlushAsync(stoppingToken);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
