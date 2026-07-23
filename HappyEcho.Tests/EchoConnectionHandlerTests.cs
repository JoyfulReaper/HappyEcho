using JoyfulReaperLib.MissionControl;
using JoyfulReaperLib.TcpServer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho.Tests;

public class EchoConnectionHandlerTests
{
    private static readonly IPEndPoint Remote =
        new(IPAddress.Parse("203.0.113.10"), 54321);
    private static readonly IPEndPoint Local =
        new(IPAddress.Parse("127.0.0.1"), 7);

    [Fact]
    public async Task EchoAsync_ReturnsInputBytesUnchangedAndCountsThem()
    {
        byte[] input = [1, 2, 3, 4, 5];
        var stream = new ScriptedStream(input);

        long count = await EchoConnectionHandler.EchoAsync(
            stream,
            requestTimeoutSeconds: 15,
            maxBytesPerConnection: 100,
            CancellationToken.None);

        Assert.Equal(input.Length, count);
        Assert.Equal(input, stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_EmptyDisconnectedInputReturnsZero()
    {
        var stream = new ScriptedStream();

        long count = await EchoConnectionHandler.EchoAsync(
            stream,
            requestTimeoutSeconds: 15,
            maxBytesPerConnection: 100,
            CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Empty(stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_EnforcesMaxBytesPerConnection()
    {
        var stream = new ScriptedStream("abcdef"u8.ToArray());

        long count = await EchoConnectionHandler.EchoAsync(
            stream,
            requestTimeoutSeconds: 15,
            maxBytesPerConnection: 3,
            CancellationToken.None);

        Assert.Equal(3, count);
        Assert.Equal("abc"u8.ToArray(), stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_WritesWithoutFlushing()
    {
        var stream = new ScriptedStream("abc"u8.ToArray(), "def"u8.ToArray())
        {
            ThrowOnFlushNumber = 1,
            FlushException = new InvalidOperationException("EchoAsync should not flush the stream.")
        };
        var state = new EchoSessionState();

        await EchoConnectionHandler.EchoAsync(
            stream,
            requestTimeoutSeconds: 15,
            maxBytesPerConnection: 100,
            CancellationToken.None,
            state);

        Assert.Equal(6, state.BytesEchoed);
        Assert.Equal("abcdef"u8.ToArray(), stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_ReportsPartialByteCountAfterWriteFailure()
    {
        var stream = new ScriptedStream("abc"u8.ToArray(), "def"u8.ToArray())
        {
            ThrowOnWriteNumber = 2,
            WriteException = new IOException("write failed")
        };
        var state = new EchoSessionState();

        await Assert.ThrowsAsync<IOException>(() =>
            EchoConnectionHandler.EchoAsync(
                stream,
                requestTimeoutSeconds: 15,
                maxBytesPerConnection: 100,
                CancellationToken.None,
                state));

        Assert.Equal(3, state.BytesEchoed);
        Assert.Equal("abc"u8.ToArray(), stream.WrittenBytes);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsClientDisconnectedOutcome()
    {
        var stream = new ScriptedStream("hello"u8.ToArray());

        EchoProtocolResult result = await ProcessAsync(stream);

        Assert.Equal(5, result.BytesEchoed);
        Assert.True(result.DurationMilliseconds >= 0);
        Assert.Equal("client-disconnected", result.Outcome);
        Assert.True(result.Succeeded);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsByteLimitReachedOutcome()
    {
        var stream = new ScriptedStream("hello"u8.ToArray());

        EchoProtocolResult result = await ProcessAsync(
            stream,
            maxBytesPerConnection: 3);

        Assert.Equal(3, result.BytesEchoed);
        Assert.Equal("byte-limit-reached", result.Outcome);
        Assert.True(result.Succeeded);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsTimeoutOutcome()
    {
        var stream = new BlockingReadStream();

        EchoProtocolResult result = await ProcessAsync(
            stream,
            requestTimeoutSeconds: 0);

        Assert.Equal("timeout", result.Outcome);
        Assert.False(result.Succeeded);
        Assert.Equal(0, result.BytesEchoed);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsIoErrorOutcomeWithPartialCount()
    {
        var stream = new ScriptedStream("abc"u8.ToArray(), "def"u8.ToArray())
        {
            ThrowOnWriteNumber = 2,
            WriteException = new IOException("broken pipe")
        };

        EchoProtocolResult result = await ProcessAsync(stream);

        Assert.Equal("io-error", result.Outcome);
        Assert.False(result.Succeeded);
        Assert.Equal(3, result.BytesEchoed);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsSocketErrorOutcome()
    {
        var stream = new ThrowingReadStream(
            new SocketException((int)SocketError.ConnectionReset));

        EchoProtocolResult result = await ProcessAsync(stream);

        Assert.Equal("socket-error", result.Outcome);
        Assert.False(result.Succeeded);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsFailedOutcome()
    {
        var stream = new ThrowingReadStream(
            new InvalidOperationException("unexpected"));

        EchoProtocolResult result = await ProcessAsync(stream);

        Assert.Equal("failed", result.Outcome);
        Assert.False(result.Succeeded);
    }

    [Fact]
    public async Task ProcessAsync_ReturnsServerShutdownOutcome()
    {
        var stream = new BlockingReadStream();
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();

        EchoProtocolResult result = await ProcessAsync(
            stream,
            cancellationToken: shutdown.Token);

        Assert.Equal("server-shutdown", result.Outcome);
        Assert.False(result.Succeeded);
    }

    [Fact]
    public async Task HandleAsync_MatchingIgnoredAddressSuppressesBothEventsAndStillEchoes()
    {
        var recording = new RecordingMissionControlClient();
        EchoConnectionHandler handler = CreateHandler(
            recording,
            telemetryIgnoredRemoteAddress: "172.21.0.1");
        var stream = new ScriptedStream("monitor"u8.ToArray());
        var remote = new IPEndPoint(IPAddress.Parse("172.21.0.1"), 54321);

        await handler.HandleAsync(
            CreateContext(stream, remote),
            CancellationToken.None);

        Assert.Equal("monitor"u8.ToArray(), stream.WrittenBytes);
        Assert.Empty(recording.PublishedEvents);
    }

    [Fact]
    public async Task HandleAsync_Ipv4MappedIpv6IgnoredAddressSuppressesBothEvents()
    {
        var recording = new RecordingMissionControlClient();
        EchoConnectionHandler handler = CreateHandler(
            recording,
            telemetryIgnoredRemoteAddress: "172.21.0.1");
        var stream = new ScriptedStream("mapped"u8.ToArray());
        var remote = new IPEndPoint(
            IPAddress.Parse("::ffff:172.21.0.1"),
            54321);

        await handler.HandleAsync(
            CreateContext(stream, remote),
            CancellationToken.None);

        Assert.Equal("mapped"u8.ToArray(), stream.WrittenBytes);
        Assert.Empty(recording.PublishedEvents);
    }

    [Fact]
    public async Task HandleAsync_IgnoredTimedOutSessionPublishesNoEvents()
    {
        var recording = new RecordingMissionControlClient();
        EchoConnectionHandler handler = CreateHandler(
            recording,
            requestTimeoutSeconds: 0,
            telemetryIgnoredRemoteAddress: "172.21.0.1");
        var stream = new BlockingReadStream();
        var remote = new IPEndPoint(IPAddress.Parse("172.21.0.1"), 54321);

        await handler.HandleAsync(
            CreateContext(stream, remote),
            CancellationToken.None);

        Assert.Empty(recording.PublishedEvents);
    }

    [Fact]
    public async Task HandleAsync_IgnoredIoFailurePublishesNoEvents()
    {
        var recording = new RecordingMissionControlClient();
        EchoConnectionHandler handler = CreateHandler(
            recording,
            telemetryIgnoredRemoteAddress: "172.21.0.1");
        var stream = new ScriptedStream("abc"u8.ToArray(), "def"u8.ToArray())
        {
            ThrowOnWriteNumber = 2,
            WriteException = new IOException("broken pipe")
        };
        var remote = new IPEndPoint(IPAddress.Parse("172.21.0.1"), 54321);

        await handler.HandleAsync(
            CreateContext(stream, remote),
            CancellationToken.None);

        Assert.Equal("abc"u8.ToArray(), stream.WrittenBytes);
        Assert.Empty(recording.PublishedEvents);
    }

    private static ValueTask<EchoProtocolResult> ProcessAsync(
        Stream stream,
        int requestTimeoutSeconds = 15,
        long maxBytesPerConnection = 1_048_576,
        CancellationToken cancellationToken = default) =>
        EchoConnectionHandler.ProcessAsync(
            stream,
            Remote,
            new HappyEchoOptions
            {
                RequestTimeoutSeconds = requestTimeoutSeconds,
                MaxBytesPerConnection = maxBytesPerConnection
            },
            NullLogger<EchoConnectionHandler>.Instance,
            cancellationToken);

    private static EchoConnectionHandler CreateHandler(
        IMissionControlClient missionControlClient,
        int requestTimeoutSeconds = 15,
        long maxBytesPerConnection = 1_048_576,
        string? telemetryIgnoredRemoteAddress = null) =>
        new(
            NullLogger<EchoConnectionHandler>.Instance,
            missionControlClient,
            Options.Create(new HappyEchoOptions
            {
                ListenAddress = "0.0.0.0",
                Port = 7,
                MaxConcurrentConnections = 64,
                RequestTimeoutSeconds = requestTimeoutSeconds,
                MaxBytesPerConnection = maxBytesPerConnection,
                TelemetryIgnoredRemoteAddress = telemetryIgnoredRemoteAddress
            }));

    private static TcpConnectionContext CreateContext(
        Stream stream,
        EndPoint remote) =>
        new(
            connectionId: 1,
            stream: stream,
            remoteEndPoint: remote,
            localEndPoint: Local,
            acceptedAt: DateTimeOffset.UtcNow);

    private sealed class ScriptedStream(params byte[][] reads) : Stream
    {
        private readonly Queue<byte[]> _reads = new(reads);
        private readonly MemoryStream _written = new();
        private readonly TaskCompletionSource _writeSignal =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _flushCount;
        private int _writeCount;

        public int? ThrowOnFlushNumber { get; init; }
        public Exception? FlushException { get; init; }
        public int? ThrowOnWriteNumber { get; init; }
        public Exception? WriteException { get; init; }
        public byte[] WrittenBytes => _written.ToArray();

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public Task WaitForWriteAsync() => _writeSignal.Task;

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_reads.Count == 0)
            {
                return ValueTask.FromResult(0);
            }

            byte[] next = _reads.Dequeue();
            int count = Math.Min(next.Length, buffer.Length);
            next.AsMemory(0, count).CopyTo(buffer);
            return ValueTask.FromResult(count);
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _writeCount++;

            if (ThrowOnWriteNumber == _writeCount && WriteException is not null)
            {
                throw WriteException;
            }

            _written.Write(buffer.Span);
            _writeSignal.TrySetResult();
            return ValueTask.CompletedTask;
        }

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _flushCount++;

            if (ThrowOnFlushNumber == _flushCount && FlushException is not null)
            {
                throw FlushException;
            }

            return Task.CompletedTask;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }

    private sealed class BlockingReadStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }

    private sealed class ThrowingReadStream(Exception exception) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException<int>(exception);

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }
}
