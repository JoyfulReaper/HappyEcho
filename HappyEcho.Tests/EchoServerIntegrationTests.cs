using HappyEcho.Events;
using JoyfulReaperLib.MissionControl;
using JoyfulReaperLib.TcpServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Sockets;
using System.Text.Json.Serialization.Metadata;

namespace HappyEcho.Tests;

public class EchoServerIntegrationTests
{
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan HostTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task RealEchoRoundTrip_EchoesExactBytesAndPublishesTelemetry()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                RequestTimeoutSeconds = 17,
                MaxBytesPerConnection = 12345
            });

        byte[] payload = [0, 1, 2, 3, 5, 8, 13, 21, 34];
        using TcpClient client = await ConnectAsync(server.Port);
        NetworkStream stream = client.GetStream();

        await WriteAllAsync(stream, payload);
        ShutdownSend(client);
        byte[] echoed = await ReadUntilEofAsync(stream);

        Assert.Equal(payload, echoed);

        await missionControl.WaitForSuccessfulCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);

        RecordedMissionControlEvent startedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        RecordedMissionControlEvent stoppedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);

        Assert.False(string.IsNullOrWhiteSpace(startedTelemetry.CorrelationId));
        Assert.Equal(startedTelemetry.CorrelationId, stoppedTelemetry.CorrelationId);

        var started = Assert.IsType<StreamingStartedEvent>(startedTelemetry.Payload);
        Assert.StartsWith("127.0.0.1:", started.Remote);
        Assert.Equal(17, started.RequestTimeoutSeconds);
        Assert.Equal(12345, started.MaxBytesPerConnection);
        Assert.Equal(
            typeof(StreamingStartedEvent),
            startedTelemetry.PayloadTypeInfo.Type);

        var stopped = Assert.IsType<StreamingStoppedEvent>(stoppedTelemetry.Payload);
        Assert.Equal(payload.Length, stopped.BytesEchoed);
        Assert.Equal("client-disconnected", stopped.Outcome);
        Assert.True(stopped.Succeeded);
    }

    [Fact]
    public async Task MaximumByteEnforcement_EchoesPrefixThenCloses()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                MaxBytesPerConnection = 4
            });

        using TcpClient client = await ConnectAsync(server.Port);
        NetworkStream stream = client.GetStream();

        await WriteAllAsync(stream, "abcd"u8.ToArray());
        byte[] echoed = await ReadExactPrefixAsync(stream, 4);
        await TryWriteAsync(stream, "ef"u8.ToArray());
        await AssertServerClosedOrResetAsync(stream);

        Assert.Equal("abcd"u8.ToArray(), echoed);

        RecordedMissionControlEvent stoppedTelemetry =
            await WaitForSingleSuccessfulStoppedAsync(missionControl);
        var stopped = Assert.IsType<StreamingStoppedEvent>(stoppedTelemetry.Payload);
        Assert.Equal(4, stopped.BytesEchoed);
        Assert.Equal("byte-limit-reached", stopped.Outcome);
        Assert.True(stopped.Succeeded);
    }

    [Fact]
    public async Task IdleTimeout_ClosesConnectionAndPublishesTimeoutOutcome()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                RequestTimeoutSeconds = 1
            });

        using TcpClient client = await ConnectAsync(server.Port);
        NetworkStream stream = client.GetStream();

        await AssertServerClosedOrResetAsync(stream, TimeSpan.FromSeconds(4));

        RecordedMissionControlEvent stoppedTelemetry =
            await WaitForSingleSuccessfulStoppedAsync(missionControl);
        var stopped = Assert.IsType<StreamingStoppedEvent>(stoppedTelemetry.Payload);
        Assert.Equal("timeout", stopped.Outcome);
        Assert.False(stopped.Succeeded);
        Assert.Equal(0, stopped.BytesEchoed);
    }

    [Fact]
    public async Task ImmediateConnectionRejection_DropsSecondConnectionWithoutTelemetry()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                MaxConcurrentConnections = 1,
                RequestTimeoutSeconds = 5
            });

        using TcpClient firstClient = await ConnectAsync(server.Port);
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStarted,
            1,
            ShortTimeout);

        using TcpClient secondClient = await ConnectAsync(server.Port);
        NetworkStream secondStream = secondClient.GetStream();
        await AssertServerClosedOrResetAsync(secondStream);

        Assert.Equal(
            1,
            missionControl.AttemptedPublications.Count(
                e => e.EventType == HappyEchoEventTypes.StreamingStarted));

        byte[] firstPayload = "first-still-active"u8.ToArray();
        NetworkStream firstStream = firstClient.GetStream();
        await WriteAllAsync(firstStream, firstPayload);
        byte[] firstEcho = await ReadExactPrefixAsync(firstStream, firstPayload.Length);
        ShutdownSend(firstClient);
        await AssertServerClosedOrResetAsync(firstStream);

        Assert.Equal(firstPayload, firstEcho);

        RecordedMissionControlEvent stoppedTelemetry =
            await WaitForSingleSuccessfulStoppedAsync(missionControl);
        var stopped = Assert.IsType<StreamingStoppedEvent>(stoppedTelemetry.Payload);
        Assert.Equal(firstPayload.Length, stopped.BytesEchoed);
        Assert.Equal("client-disconnected", stopped.Outcome);
        Assert.True(stopped.Succeeded);
    }

    [Fact]
    public async Task LoopbackBlocking_DropsConnectionWithoutStreamingTelemetry()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                BlockLoopbackConnections = true
            });

        using TcpClient client = await ConnectAsync(server.Port);
        NetworkStream stream = client.GetStream();
        await AssertServerClosedOrResetAsync(stream);

        Assert.DoesNotContain(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        Assert.DoesNotContain(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
    }

    [Fact]
    public async Task StartupTelemetryFailure_DoesNotPreventServingEchoTraffic()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.ThrowFor(HappyEchoEventTypes.ServiceStarted);

        await using var server = await EchoHost.StartAsync(missionControl);

        byte[] payload = "startup-survives"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        Assert.Contains(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.ServiceStarted);
        Assert.Contains(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        Assert.Contains(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
    }

    [Fact]
    public async Task ActiveConnectionShutdown_CompletesAndPublishesServerShutdownOutcome()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                RequestTimeoutSeconds = 30
            });

        using TcpClient client = await ConnectAsync(server.Port);
        NetworkStream stream = client.GetStream();
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStarted,
            1,
            ShortTimeout);

        await server.StopAsync(HostTimeout);
        await AssertServerClosedOrResetAsync(stream);

        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);
        RecordedMissionControlEvent stoppedTelemetry = Assert.Single(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
        var stopped = Assert.IsType<StreamingStoppedEvent>(stoppedTelemetry.Payload);
        Assert.Equal("server-shutdown", stopped.Outcome);
        Assert.False(stopped.Succeeded);
    }

    [Fact]
    public async Task PortRelease_AllowsImmediateListenerReuse()
    {
        int port = AllocateTemporaryLoopbackPort();
        var missionControl = new IntegrationMissionControlClient();
        await using (var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = port
            }))
        {
            Assert.Equal(port, server.Port);
            await server.StopAsync(HostTimeout);
        }

        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        listener.Stop();
    }

    [Fact]
    public async Task TelemetryMustNotHoldConnectionSlot()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.StreamingStopped);

        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                MaxConcurrentConnections = 1
            });

        try
        {
            byte[] firstPayload = "one"u8.ToArray();
            using TcpClient firstClient = await ConnectAsync(server.Port);
            NetworkStream firstStream = firstClient.GetStream();
            await WriteAllAsync(firstStream, firstPayload);
            ShutdownSend(firstClient);

            byte[] firstEcho = await ReadUntilEofAsync(firstStream);
            Assert.Equal(firstPayload, firstEcho);

            await missionControl.WaitForAttemptCountAsync(
                HappyEchoEventTypes.StreamingStopped,
                1,
                ShortTimeout);
            Assert.Equal(0, missionControl.SuccessfulCount(HappyEchoEventTypes.StreamingStopped));

            byte[] secondPayload = "two"u8.ToArray();
            byte[] secondEcho = await EchoRoundTripAsync(server.Port, secondPayload);

            Assert.Equal(secondPayload, secondEcho);
            await missionControl.WaitForAttemptCountAsync(
                HappyEchoEventTypes.StreamingStopped,
                2,
                ShortTimeout);
        }
        finally
        {
            missionControl.ReleaseBlockedPublications(HappyEchoEventTypes.StreamingStopped);
        }

        await missionControl.WaitForSuccessfulCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            2,
            ShortTimeout);
    }

    [Fact]
    public async Task StartedTelemetryFailure_DoesNotSuppressStoppedTelemetry()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.ThrowFor(HappyEchoEventTypes.StreamingStarted);
        await using var server = await EchoHost.StartAsync(missionControl);

        byte[] payload = "safe"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);
        Assert.Contains(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        Assert.Contains(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
    }

    [Fact]
    public async Task StoppedTelemetryFailure_DoesNotKillServer()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.ThrowFor(HappyEchoEventTypes.StreamingStopped);
        await using var server = await EchoHost.StartAsync(missionControl);

        Assert.Equal("safe"u8.ToArray(), await EchoRoundTripAsync(server.Port, "safe"u8.ToArray()));
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);

        Assert.Equal("again"u8.ToArray(), await EchoRoundTripAsync(server.Port, "again"u8.ToArray()));
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            2,
            ShortTimeout);
    }

    [Fact]
    public async Task BlockedStartedTelemetry_DoesNotDelayEchoTraffic()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.StreamingStarted);
        await using var server = await EchoHost.StartAsync(missionControl);

        try
        {
            byte[] payload = "fast"u8.ToArray();
            byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

            Assert.Equal(payload, echoed);
            await missionControl.WaitForAttemptCountAsync(
                HappyEchoEventTypes.StreamingStarted,
                1,
                ShortTimeout);
            Assert.Equal(0, missionControl.SuccessfulCount(HappyEchoEventTypes.StreamingStarted));
        }
        finally
        {
            missionControl.ReleaseBlockedPublications(HappyEchoEventTypes.StreamingStarted);
        }
    }

    [Fact]
    public async Task StreamingTelemetry_DoesNotPublishEchoedPayloadContent()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(missionControl);

        byte[] payload = "secret-message"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForSuccessfulCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);
        Assert.All(missionControl.SuccessfulPublications, telemetry =>
        {
            string text = telemetry.Payload?.ToString() ?? string.Empty;
            Assert.DoesNotContain("secret-message", text);
        });
    }

    [Fact]
    public async Task MatchingIgnoredAddress_SuppressesStreamingEventsAndStillEchoes()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                TelemetryIgnoredRemoteAddress = "127.0.0.1"
            });

        byte[] payload = "monitor"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        Assert.DoesNotContain(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        Assert.DoesNotContain(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
    }

    [Fact]
    public async Task NonMatchingIgnoredAddress_PublishesStreamingPair()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                TelemetryIgnoredRemoteAddress = "172.21.0.1"
            });

        byte[] payload = "real"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForSuccessfulCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);

        RecordedMissionControlEvent startedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        RecordedMissionControlEvent stoppedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);

        Assert.False(string.IsNullOrWhiteSpace(startedTelemetry.CorrelationId));
        Assert.Equal(startedTelemetry.CorrelationId, stoppedTelemetry.CorrelationId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task MissingIgnoredAddress_DoesNotSuppressStreamingTelemetry(
        string? telemetryIgnoredRemoteAddress)
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                TelemetryIgnoredRemoteAddress = telemetryIgnoredRemoteAddress
            });

        byte[] payload = "normal"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForSuccessfulCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);
        Assert.Contains(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStarted);
        Assert.Contains(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
    }

    [Fact]
    public async Task StartedTelemetryTimeout_IsBoundedAndStoppedTelemetryIsAttempted()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.StreamingStarted);
        await using var server = await EchoHost.StartAsync(missionControl);

        byte[] payload = "safe"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForCanceledCountAsync(
            HappyEchoEventTypes.StreamingStarted,
            1,
            TimeSpan.FromSeconds(4));
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);
    }

    [Fact]
    public async Task StoppedTelemetryTimeout_IsBounded()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.StreamingStopped);
        await using var server = await EchoHost.StartAsync(missionControl);

        byte[] payload = "safe"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForCanceledCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task StartupTelemetryTimeout_DoesNotPreventAcceptingConnections()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.ServiceStarted);
        await using var server = await EchoHost.StartAsync(
            missionControl,
            timeout: TimeSpan.FromSeconds(4));

        byte[] payload = "startup"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);

        Assert.Equal(payload, echoed);
        await missionControl.WaitForCanceledCountAsync(
            HappyEchoEventTypes.ServiceStarted,
            1,
            TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task ShutdownCompletesWithBlockedStoppedTelemetry()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.StreamingStopped);
        await using var server = await EchoHost.StartAsync(missionControl);

        byte[] payload = "stop"u8.ToArray();
        byte[] echoed = await EchoRoundTripAsync(server.Port, payload);
        await missionControl.WaitForAttemptCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);

        await server.StopAsync(ShortTimeout);

        Assert.Equal(payload, echoed);
        Assert.True(server.Stopped);

        missionControl.ReleaseBlockedPublications(HappyEchoEventTypes.StreamingStopped);
    }

    private static async Task<RecordedMissionControlEvent> WaitForSingleSuccessfulStoppedAsync(
        IntegrationMissionControlClient missionControl)
    {
        await missionControl.WaitForSuccessfulCountAsync(
            HappyEchoEventTypes.StreamingStopped,
            1,
            ShortTimeout);

        return Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.StreamingStopped);
    }

    private static async Task<byte[]> EchoRoundTripAsync(
        int port,
        byte[] payload)
    {
        using TcpClient client = await ConnectAsync(port);
        NetworkStream stream = client.GetStream();
        await WriteAllAsync(stream, payload);
        ShutdownSend(client);
        return await ReadUntilEofAsync(stream);
    }

    private static async Task<TcpClient> ConnectAsync(
        int port,
        TimeSpan? timeout = null)
    {
        var client = new TcpClient(AddressFamily.InterNetwork);
        try
        {
            await client.ConnectAsync(IPAddress.Loopback, port).WaitAsync(
                timeout ?? ShortTimeout);
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    private static async Task WriteAllAsync(
        NetworkStream stream,
        byte[] payload,
        TimeSpan? timeout = null) =>
        await stream.WriteAsync(payload).AsTask().WaitAsync(timeout ?? ShortTimeout);

    private static async Task TryWriteAsync(
        NetworkStream stream,
        byte[] payload,
        TimeSpan? timeout = null)
    {
        try
        {
            await WriteAllAsync(stream, payload, timeout);
        }
        catch (IOException)
        {
        }
        catch (SocketException)
        {
        }
    }

    private static void ShutdownSend(TcpClient client) =>
        client.Client.Shutdown(SocketShutdown.Send);

    private static async Task<byte[]> ReadExactPrefixAsync(
        NetworkStream stream,
        int count,
        TimeSpan? timeout = null)
    {
        byte[] buffer = new byte[count];
        int offset = 0;

        while (offset < count)
        {
            int read = await stream.ReadAsync(
                buffer.AsMemory(offset, count - offset)).AsTask().WaitAsync(
                    timeout ?? ShortTimeout);

            if (read == 0)
            {
                throw new EndOfStreamException(
                    $"Expected {count} bytes but received {offset}.");
            }

            offset += read;
        }

        return buffer;
    }

    private static async Task<byte[]> ReadUntilEofAsync(
        NetworkStream stream,
        TimeSpan? timeout = null)
    {
        using var output = new MemoryStream();
        byte[] buffer = new byte[512];

        while (true)
        {
            int read = await stream.ReadAsync(buffer).AsTask().WaitAsync(
                timeout ?? ShortTimeout);

            if (read == 0)
            {
                return output.ToArray();
            }

            output.Write(buffer, 0, read);
        }
    }

    private static async Task AssertServerClosedOrResetAsync(
        NetworkStream stream,
        TimeSpan? timeout = null)
    {
        byte[] buffer = new byte[1];

        try
        {
            int read = await stream.ReadAsync(buffer).AsTask().WaitAsync(
                timeout ?? ShortTimeout);
            Assert.Equal(0, read);
        }
        catch (IOException)
        {
        }
        catch (SocketException)
        {
        }
    }

    private static int AllocateTemporaryLoopbackPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class EchoHost : IAsyncDisposable
    {
        private readonly IHost _host;
        private int _stopped;

        private EchoHost(IHost host, int port)
        {
            _host = host;
            Port = port;
        }

        public int Port { get; }
        public bool Stopped => Volatile.Read(ref _stopped) == 1;

        public static async Task<EchoHost> StartAsync(
            IntegrationMissionControlClient missionControl,
            HappyEchoOptions? options = null,
            TimeSpan? timeout = null)
        {
            HappyEchoOptions testOptions = options ?? new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0
            };
            int port = testOptions.Port == 0
                ? AllocateTemporaryLoopbackPort()
                : testOptions.Port;

            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IMissionControlClient>(missionControl);

            builder.Services.Configure<HappyEchoOptions>(configured =>
            {
                configured.ListenAddress = testOptions.ListenAddress;
                configured.Port = port;
                configured.MaxConcurrentConnections = testOptions.MaxConcurrentConnections;
                configured.RequestTimeoutSeconds = testOptions.RequestTimeoutSeconds;
                configured.MaxBytesPerConnection = testOptions.MaxBytesPerConnection;
                configured.TelemetryIgnoredRemoteAddress = testOptions.TelemetryIgnoredRemoteAddress;
                configured.BlockLoopbackConnections = testOptions.BlockLoopbackConnections;
            });

            builder.Services.AddTcpServer<EchoConnectionHandler, HappyEchoOptions>();
            builder.Services.AddHostedService<EchoLifecycleService>();

            IHost host = builder.Build();
            var echoHost = new EchoHost(host, port);

            try
            {
                await host.StartAsync().WaitAsync(timeout ?? HostTimeout);
                await missionControl.WaitForAttemptCountAsync(
                    HappyEchoEventTypes.ServiceStarted,
                    1,
                    timeout ?? HostTimeout);

                return echoHost;
            }
            catch
            {
                await echoHost.DisposeAsync();
                throw;
            }
        }

        public async Task StopAsync(TimeSpan? timeout = null)
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 1)
            {
                return;
            }

            try
            {
                await _host.StopAsync(timeout ?? HostTimeout).WaitAsync(
                    timeout ?? HostTimeout);
            }
            finally
            {
                _host.Dispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync(HostTimeout);
        }
    }

    private sealed class IntegrationMissionControlClient : IMissionControlClient
    {
        private readonly object _gate = new();
        private readonly HashSet<string> _throwingEventTypes = [];
        private readonly Dictionary<string, int> _remainingThrows = [];
        private readonly Dictionary<string, BlockedPublication> _blockedEventTypes = [];
        private readonly List<RecordedMissionControlEvent> _attempted = [];
        private readonly List<RecordedMissionControlEvent> _successful = [];
        private readonly Dictionary<string, int> _canceledCounts = [];
        private TaskCompletionSource _changed =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IReadOnlyList<RecordedMissionControlEvent> AttemptedPublications
        {
            get
            {
                lock (_gate)
                {
                    return _attempted.ToArray();
                }
            }
        }

        public IReadOnlyList<RecordedMissionControlEvent> SuccessfulPublications
        {
            get
            {
                lock (_gate)
                {
                    return _successful.ToArray();
                }
            }
        }

        public void ThrowFor(string eventType)
        {
            lock (_gate)
            {
                _throwingEventTypes.Add(eventType);
            }
        }

        public void ThrowFor(string eventType, int count)
        {
            lock (_gate)
            {
                _remainingThrows[eventType] = count;
            }
        }

        public void Block(string eventType)
        {
            lock (_gate)
            {
                _blockedEventTypes[eventType] = new BlockedPublication();
            }
        }

        public void ReleaseBlockedPublications(string eventType)
        {
            lock (_gate)
            {
                if (_blockedEventTypes.TryGetValue(
                    eventType,
                    out BlockedPublication? blocked))
                {
                    blocked.Release.TrySetResult();
                }
            }
        }

        public int SuccessfulCount(string eventType)
        {
            lock (_gate)
            {
                return _successful.Count(e => e.EventType == eventType);
            }
        }

        public int CanceledCount(string eventType)
        {
            lock (_gate)
            {
                return _canceledCounts.TryGetValue(eventType, out int count)
                    ? count
                    : 0;
            }
        }

        public async Task<bool> TryPublishAsync<TPayload>(
            string eventType,
            TPayload payload,
            JsonTypeInfo<TPayload> payloadTypeInfo,
            DateTimeOffset occurredAt,
            string? correlationId = null,
            CancellationToken cancellationToken = default)
        {
            var publication = new RecordedMissionControlEvent(
                eventType,
                payload,
                occurredAt,
                correlationId,
                payloadTypeInfo);
            BlockedPublication? blocked = null;

            lock (_gate)
            {
                _attempted.Add(publication);
                if (ShouldThrow(eventType))
                {
                    SignalChanged();
                    throw new InvalidOperationException("Configured telemetry failure.");
                }

                _blockedEventTypes.TryGetValue(eventType, out blocked);
                SignalChanged();
            }

            if (blocked is not null)
            {
                try
                {
                    await blocked.Release.Task.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    lock (_gate)
                    {
                        _canceledCounts[eventType] =
                            _canceledCounts.GetValueOrDefault(eventType) + 1;
                        SignalChanged();
                    }

                    throw;
                }
            }

            lock (_gate)
            {
                _successful.Add(publication);
                SignalChanged();
            }

            return true;
        }

        public Task WaitForAttemptAsync(
            string eventType,
            TimeSpan timeout) =>
            WaitUntilAsync(
                () => _attempted.Any(e => e.EventType == eventType),
                timeout);

        public Task WaitForSuccessfulAsync(
            string eventType,
            TimeSpan timeout) =>
            WaitUntilAsync(
                () => _successful.Any(e => e.EventType == eventType),
                timeout);

        public Task WaitForAttemptCountAsync(
            string eventType,
            int expectedCount,
            TimeSpan timeout) =>
            WaitUntilAsync(
                () => _attempted.Count(e => e.EventType == eventType) >= expectedCount,
                timeout);

        public Task WaitForSuccessfulCountAsync(
            string eventType,
            int expectedCount,
            TimeSpan timeout) =>
            WaitUntilAsync(
                () => _successful.Count(e => e.EventType == eventType) >= expectedCount,
                timeout);

        public Task WaitForCanceledCountAsync(
            string eventType,
            int expectedCount,
            TimeSpan timeout) =>
            WaitUntilAsync(
                () => _canceledCounts.GetValueOrDefault(eventType) >= expectedCount,
                timeout);

        private async Task WaitUntilAsync(
            Func<bool> condition,
            TimeSpan timeout)
        {
            using var cancellation = new CancellationTokenSource(timeout);

            while (true)
            {
                Task signalTask;
                lock (_gate)
                {
                    if (condition())
                    {
                        return;
                    }

                    signalTask = _changed.Task;
                }

                await signalTask.WaitAsync(cancellation.Token);
            }
        }

        private void SignalChanged()
        {
            TaskCompletionSource previous = _changed;
            _changed = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            previous.TrySetResult();
        }

        private bool ShouldThrow(string eventType)
        {
            if (_throwingEventTypes.Contains(eventType))
            {
                return true;
            }

            if (!_remainingThrows.TryGetValue(eventType, out int remaining) ||
                remaining <= 0)
            {
                return false;
            }

            if (remaining == 1)
            {
                _remainingThrows.Remove(eventType);
            }
            else
            {
                _remainingThrows[eventType] = remaining - 1;
            }

            return true;
        }

        private sealed class BlockedPublication
        {
            public TaskCompletionSource Release { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    private sealed record RecordedMissionControlEvent(
        string EventType,
        object? Payload,
        DateTimeOffset OccurredAt,
        string? CorrelationId,
        JsonTypeInfo PayloadTypeInfo);
}
