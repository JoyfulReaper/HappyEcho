using HappyEcho;
using JoyfulReaperLib.MissionControl;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho.Tests;

public class EchoWorkerTests
{
    private static readonly IPEndPoint Remote =
        new(IPAddress.Parse("203.0.113.10"), 54321);

    [Fact]
    public async Task EchoAsync_ReturnsInputBytesUnchangedAndCountsThem()
    {
        byte[] input = [1, 2, 3, 4, 5];
        var stream = new ScriptedStream(input);

        long count = await EchoWorker.EchoAsync(
            stream,
            RequestTimeoutSeconds: 15,
            maxBytesPerConnection: 100,
            CancellationToken.None);

        Assert.Equal(input.Length, count);
        Assert.Equal(input, stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_EmptyDisconnectedInputReturnsZero()
    {
        var stream = new ScriptedStream();

        long count = await EchoWorker.EchoAsync(
            stream,
            RequestTimeoutSeconds: 15,
            maxBytesPerConnection: 100,
            CancellationToken.None);

        Assert.Equal(0, count);
        Assert.Empty(stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_EnforcesMaxBytesPerConnection()
    {
        var stream = new ScriptedStream("abcdef"u8.ToArray());

        long count = await EchoWorker.EchoAsync(
            stream,
            RequestTimeoutSeconds: 15,
            maxBytesPerConnection: 3,
            CancellationToken.None);

        Assert.Equal(3, count);
        Assert.Equal("abc"u8.ToArray(), stream.WrittenBytes);
    }

    [Fact]
    public async Task EchoAsync_CountsOnlySuccessfullyWrittenBytes()
    {
        var stream = new ScriptedStream("abc"u8.ToArray(), "def"u8.ToArray())
        {
            ThrowOnWriteNumber = 2,
            WriteException = new IOException("write failed")
        };
        var state = new EchoSessionState();

        await Assert.ThrowsAsync<IOException>(() =>
            EchoWorker.EchoAsync(
                stream,
                RequestTimeoutSeconds: 15,
                maxBytesPerConnection: 100,
                CancellationToken.None,
                state));

        Assert.Equal(3, state.BytesEchoed);
        Assert.Equal("abc"u8.ToArray(), stream.WrittenBytes);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_PublishesStartedAndStoppedForSuccessfulEcho()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording);
        var stream = new ScriptedStream("hello"u8.ToArray());

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        RecordedMissionControlEvent[] events = recording.PublishedEvents.ToArray();
        Assert.Equal(2, events.Length);
        Assert.Equal(HappyEchoEventTypes.StreamingStarted, events[0].EventType);
        Assert.Equal(HappyEchoEventTypes.StreamingStopped, events[1].EventType);
        Assert.False(string.IsNullOrWhiteSpace(events[0].CorrelationId));
        Assert.Equal(events[0].CorrelationId, events[1].CorrelationId);

        var stopped = Assert.IsType<StreamingStoppedEvent>(events[1].Payload);
        Assert.Equal(5, stopped.BytesEchoed);
        Assert.True(stopped.DurationMilliseconds >= 0);
        Assert.Equal("client-disconnected", stopped.Outcome);
        Assert.True(stopped.Succeeded);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_PublishesByteLimitReachedOutcome()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording, maxBytesPerConnection: 3);
        var stream = new ScriptedStream("hello"u8.ToArray());

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        var stopped = Assert.IsType<StreamingStoppedEvent>(
            recording.PublishedEvents[1].Payload);

        Assert.Equal(3, stopped.BytesEchoed);
        Assert.Equal("byte-limit-reached", stopped.Outcome);
        Assert.True(stopped.Succeeded);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_TimeoutProducesTimeoutOutcome()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording, requestTimeoutSeconds: 0);
        var stream = new BlockingReadStream();

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        var stopped = Assert.IsType<StreamingStoppedEvent>(
            recording.PublishedEvents[1].Payload);

        Assert.Equal("timeout", stopped.Outcome);
        Assert.False(stopped.Succeeded);
        Assert.Equal(0, stopped.BytesEchoed);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_IOExceptionProducesIoErrorOutcomeWithPartialCount()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording);
        var stream = new ScriptedStream("abc"u8.ToArray(), "def"u8.ToArray())
        {
            ThrowOnWriteNumber = 2,
            WriteException = new IOException("broken pipe")
        };

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        var stopped = Assert.IsType<StreamingStoppedEvent>(
            recording.PublishedEvents[1].Payload);

        Assert.Equal("io-error", stopped.Outcome);
        Assert.False(stopped.Succeeded);
        Assert.Equal(3, stopped.BytesEchoed);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_SocketExceptionProducesSocketErrorOutcome()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording);
        var stream = new ThrowingReadStream(
            new SocketException((int)SocketError.ConnectionReset));

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        var stopped = Assert.IsType<StreamingStoppedEvent>(
            recording.PublishedEvents[1].Payload);

        Assert.Equal("socket-error", stopped.Outcome);
        Assert.False(stopped.Succeeded);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_UnexpectedExceptionProducesFailedOutcome()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording);
        var stream = new ThrowingReadStream(
            new InvalidOperationException("unexpected"));

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        var stopped = Assert.IsType<StreamingStoppedEvent>(
            recording.PublishedEvents[1].Payload);

        Assert.Equal("failed", stopped.Outcome);
        Assert.False(stopped.Succeeded);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_ShutdownCancellationProducesServerShutdownOutcome()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording);
        var stream = new BlockingReadStream();
        using var shutdown = new CancellationTokenSource();
        await shutdown.CancelAsync();

        await worker.ProcessEchoSessionAsync(stream, Remote, shutdown.Token);

        var stopped = Assert.IsType<StreamingStoppedEvent>(
            recording.PublishedEvents[1].Payload);

        Assert.Equal("server-shutdown", stopped.Outcome);
        Assert.False(stopped.Succeeded);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_StartedTelemetryFailureDoesNotSuppressStopped()
    {
        var recording = new ThrowingByEventMissionControlClient(
            HappyEchoEventTypes.StreamingStarted);
        EchoWorker worker = CreateWorker(recording);
        var stream = new ScriptedStream("safe"u8.ToArray());

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        Assert.Equal(
            [
                HappyEchoEventTypes.StreamingStarted,
                HappyEchoEventTypes.StreamingStopped
            ],
            recording.AttemptedEventTypes);
        Assert.Equal("safe"u8.ToArray(), stream.WrittenBytes);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_StoppedTelemetryFailureDoesNotEscape()
    {
        var recording = new ThrowingByEventMissionControlClient(
            HappyEchoEventTypes.StreamingStopped);
        EchoWorker worker = CreateWorker(recording);
        var stream = new ScriptedStream("safe"u8.ToArray());

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        Assert.Equal(
            [
                HappyEchoEventTypes.StreamingStarted,
                HappyEchoEventTypes.StreamingStopped
            ],
            recording.AttemptedEventTypes);
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_StartedTelemetryDoesNotDelayEcho()
    {
        var releaseStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var recording = new RecordingMissionControlClient
        {
            OnPublishAsync = (eventType, _) =>
                eventType == HappyEchoEventTypes.StreamingStarted
                    ? releaseStarted.Task
                    : Task.CompletedTask
        };

        EchoWorker worker = CreateWorker(recording);
        var stream = new ScriptedStream("fast"u8.ToArray());

        Task sessionTask = worker.ProcessEchoSessionAsync(
            stream,
            Remote,
            CancellationToken.None);

        await stream.WaitForWriteAsync();
        Assert.Equal("fast"u8.ToArray(), stream.WrittenBytes);

        releaseStarted.SetResult();
        await sessionTask;
    }

    [Fact]
    public async Task ProcessEchoSessionAsync_DoesNotPublishEchoedPayloadContent()
    {
        var recording = new RecordingMissionControlClient();
        EchoWorker worker = CreateWorker(recording);
        var stream = new ScriptedStream("secret-message"u8.ToArray());

        await worker.ProcessEchoSessionAsync(stream, Remote, CancellationToken.None);

        Assert.All(recording.PublishedEvents, telemetry =>
        {
            string text = telemetry.Payload?.ToString() ?? string.Empty;
            Assert.DoesNotContain("secret-message", text);
        });
    }

    private static EchoWorker CreateWorker(
        IMissionControlClient missionControlClient,
        int requestTimeoutSeconds = 15,
        long maxBytesPerConnection = 1_048_576) =>
        new(
            NullLogger<EchoWorker>.Instance,
            missionControlClient,
            Options.Create(new HappyEchoOptions
            {
                ListenAddress = "0.0.0.0",
                Port = 7,
                MaxConcurrentConnections = 64,
                RequestTimeoutSeconds = requestTimeoutSeconds,
                MaxBytesPerConnection = maxBytesPerConnection
            }));

    private sealed class ThrowingByEventMissionControlClient(
        string eventTypeToThrow) : IMissionControlClient
    {
        private readonly List<string> _attemptedEventTypes = [];

        public IReadOnlyList<string> AttemptedEventTypes => _attemptedEventTypes;

        public Task<bool> TryPublishAsync<TPayload>(
            string eventType,
            TPayload payload,
            DateTimeOffset occurredAt,
            string? correlationId = null,
            CancellationToken cancellationToken = default)
        {
            _attemptedEventTypes.Add(eventType);

            if (eventType == eventTypeToThrow)
            {
                throw new InvalidOperationException("telemetry failed");
            }

            return Task.FromResult(true);
        }
    }

    private sealed class ScriptedStream(params byte[][] reads) : Stream
    {
        private readonly Queue<byte[]> _reads = new(reads);
        private readonly MemoryStream _written = new();
        private readonly TaskCompletionSource _writeSignal =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _writeCount;

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

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            Task.CompletedTask;

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
