/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using JoyfulReaperLib.TcpServer;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho;

/// <summary>
/// Processes Echo protocol connections.
/// </summary>
public sealed class EchoConnectionHandler(
    ILogger<EchoConnectionHandler> logger,
    IOptions<HappyEchoOptions> options) : ITcpConnectionHandler
{
    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TcpConnectionContext context,
        CancellationToken cancellationToken)
    {
        _ = await ProcessAsync(
            context.Stream,
            context.RemoteEndPoint,
            options.Value,
            logger,
            cancellationToken);
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
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
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

                // Count only data that was successfully written and flushed.
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