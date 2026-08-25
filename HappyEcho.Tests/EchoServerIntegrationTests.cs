using HappyEcho.Events;
using JoyfulReaperLib.MissionControl;
using JoyfulReaperLib.TcpServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Sockets;
using System.Text.Json.Serialization.Metadata;

namespace HappyEcho.Tests;

public class EchoServerIntegrationTests
{
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan HostTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task UdpImmediateStop_ReleasesPort()
    {
        int udpPort =
            AllocateTemporaryUdpPort(IPAddress.Loopback);

        var missionControl =
            new IntegrationMissionControlClient();

        using var service =
            new UdpEchoService(
                NullLogger<UdpEchoService>.Instance,
                missionControl,
                Options.Create(
                    new HappyEchoOptions
                    {
                        ListenAddress = "127.0.0.1",
                        Port = 0,
                        UdpEnabled = true,
                        UdpListenAddress = "127.0.0.1",
                        UdpPort = udpPort
                    }));

        await service.StartAsync(
            CancellationToken.None);

        await service.StopAsync(
            CancellationToken.None);

        using var udp =
            new UdpClient(
                AddressFamily.InterNetwork);

        udp.Client.Bind(
            new IPEndPoint(
                IPAddress.Loopback,
                udpPort));

        Assert.Equal(
            udpPort,
            ((IPEndPoint)udp.Client.LocalEndPoint!).Port);
    }

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

        await server.StopAsync(HostTimeout);

        Assert.Equal(payload, echoed);
        Assert.True(server.Stopped);

        missionControl.ReleaseBlockedPublications(HappyEchoEventTypes.StreamingStopped);
    }

    [Fact]
    public async Task UdpIPv4_EchoesExactDatagramAndPublishesTelemetry()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0
            });

        byte[] payload = [0, 255, 1, 254, 2, 253, 3, 252];
        byte[] echoed = await UdpEchoRoundTripAsync(
            IPAddress.Loopback,
            server.UdpPort,
            payload);

        Assert.Equal(payload, echoed);

        await missionControl.WaitForSuccessfulAsync(
            HappyEchoEventTypes.UdpDatagramEchoed,
            ShortTimeout);

        RecordedMissionControlEvent startedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.UdpStarted);
        var started = Assert.IsType<UdpEchoStartedEvent>(startedTelemetry.Payload);
        Assert.Equal($"127.0.0.1:{server.UdpPort}", started.ListenEndpoint);
        Assert.Equal(65_507, started.MaxDatagramBytes);
        Assert.False(started.BlockLoopbackConnections);
        Assert.Equal(typeof(UdpEchoStartedEvent), startedTelemetry.PayloadTypeInfo.Type);

        RecordedMissionControlEvent echoedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.UdpDatagramEchoed);
        var datagramEchoed = Assert.IsType<UdpDatagramEchoedEvent>(
            echoedTelemetry.Payload);
        Assert.StartsWith("127.0.0.1:", datagramEchoed.Remote);
        Assert.Equal(payload.Length, datagramEchoed.BytesEchoed);
        Assert.Equal(
            typeof(UdpDatagramEchoedEvent),
            echoedTelemetry.PayloadTypeInfo.Type);

        await server.StopAsync();
        await missionControl.WaitForSuccessfulAsync(
            HappyEchoEventTypes.UdpStopped,
            ShortTimeout);

        RecordedMissionControlEvent stoppedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.UdpStopped);
        var stopped = Assert.IsType<UdpEchoStoppedEvent>(stoppedTelemetry.Payload);
        Assert.Equal($"127.0.0.1:{server.UdpPort}", stopped.ListenEndpoint);
        Assert.Equal(1, stopped.DatagramsReceived);
        Assert.Equal(1, stopped.DatagramsEchoed);
        Assert.Equal(0, stopped.DatagramsDropped);
        Assert.Equal(payload.Length, stopped.BytesEchoed);
        Assert.True(stopped.DurationMilliseconds >= 0);
    }

    [Fact]
    public async Task UdpIPv6_EchoesExactDatagram()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "::1",
                UdpPort = 0
            });

        byte[] payload = [252, 3, 253, 2, 254, 1, 255, 0];
        byte[] echoed = await UdpEchoRoundTripAsync(
            IPAddress.IPv6Loopback,
            server.UdpPort,
            payload);

        Assert.Equal(payload, echoed);
    }

    [Fact]
    public async Task UdpDualMode_EchoesIPv4LoopbackDatagram()
    {
        if (!Socket.OSSupportsIPv6)
        {
            return;
        }

        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "::",
                DualMode = true,
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "::",
                UdpPort = 0
            });

        byte[] payload = "udp-dual-mode-ipv4"u8.ToArray();
        byte[] echoed = await UdpEchoRoundTripAsync(
            IPAddress.Loopback,
            server.UdpPort,
            payload);

        Assert.Equal(payload, echoed);
    }

    [Fact]
    public async Task UdpDualMode_EchoesIPv6LoopbackDatagram()
    {
        if (!Socket.OSSupportsIPv6)
        {
            return;
        }

        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "::",
                DualMode = true,
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "::",
                UdpPort = 0
            });

        byte[] payload = "udp-dual-mode-ipv6"u8.ToArray();
        byte[] echoed = await UdpEchoRoundTripAsync(
            IPAddress.IPv6Loopback,
            server.UdpPort,
            payload);

        Assert.Equal(payload, echoed);
    }

    [Fact]
    public async Task UdpDualMode_WithIPv4ListenAddress_FailsStartup()
    {
        var missionControl = new IntegrationMissionControlClient();
        using var service = new UdpEchoService(
            NullLogger<UdpEchoService>.Instance,
            missionControl,
            Options.Create(new HappyEchoOptions
            {
                DualMode = true,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0
            }));

        InvalidOperationException exception =
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => service.StartAsync(
                    CancellationToken.None));

        Assert.Equal(
            "UDP dual mode requires the UDP listen address to be the IPv6 wildcard address '::'.",
            exception.Message);
    }

    [Fact]
    public async Task UdpLoopbackBlocking_DropsDatagram()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0,
                BlockLoopbackConnections = true
            });

        using var udp = new UdpClient(AddressFamily.InterNetwork);
        udp.Connect(IPAddress.Loopback, server.UdpPort);
        byte[] payload = "blocked-loopback"u8.ToArray();
        await udp.SendAsync(payload, payload.Length).WaitAsync(ShortTimeout);

        await AssertNoUdpResponseAsync(udp, TimeSpan.FromMilliseconds(500));
        await missionControl.WaitForSuccessfulAsync(
            HappyEchoEventTypes.UdpDatagramDropped,
            ShortTimeout);

        RecordedMissionControlEvent droppedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.UdpDatagramDropped);
        var dropped = Assert.IsType<UdpDatagramDroppedEvent>(
            droppedTelemetry.Payload);
        Assert.Equal("loopback-blocked", dropped.Reason);
    }

    [Fact]
    public async Task UdpDisabled_DoesNotBindConfiguredPort()
    {
        int udpPort = AllocateTemporaryUdpPort(IPAddress.Loopback);
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = false,
                UdpListenAddress = "127.0.0.1",
                UdpPort = udpPort
            });

        using var udp = new UdpClient(AddressFamily.InterNetwork);
        udp.Client.Bind(new IPEndPoint(IPAddress.Loopback, udpPort));

        Assert.Equal(udpPort, ((IPEndPoint)udp.Client.LocalEndPoint!).Port);
    }

    [Fact]
    public async Task OversizedUdpDatagram_IsNotEchoed()
    {
        var missionControl = new IntegrationMissionControlClient();
        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0,
                MaxUdpDatagramBytes = 8
            });

        using var udp = new UdpClient(AddressFamily.InterNetwork);
        udp.Connect(IPAddress.Loopback, server.UdpPort);
        byte[] acceptedPayload = new byte[8];
        await udp.SendAsync(acceptedPayload, acceptedPayload.Length).WaitAsync(
            ShortTimeout);
        UdpReceiveResult accepted = await udp.ReceiveAsync().WaitAsync(ShortTimeout);
        Assert.Equal(acceptedPayload, accepted.Buffer);

        byte[] payload = new byte[9];
        await udp.SendAsync(payload, payload.Length).WaitAsync(ShortTimeout);

        await AssertNoUdpResponseAsync(
            udp,
            TimeSpan.FromMilliseconds(500));

        byte[] postDropPayload = [1, 2, 3, 4];
        await udp.SendAsync(postDropPayload, postDropPayload.Length).WaitAsync(
            ShortTimeout);
        UdpReceiveResult postDropEcho = await udp.ReceiveAsync().WaitAsync(
            ShortTimeout);
        Assert.Equal(postDropPayload, postDropEcho.Buffer);

        await missionControl.WaitForSuccessfulAsync(
            HappyEchoEventTypes.UdpDatagramDropped,
            ShortTimeout);

        RecordedMissionControlEvent droppedTelemetry = Assert.Single(
            missionControl.SuccessfulPublications,
            e => e.EventType == HappyEchoEventTypes.UdpDatagramDropped);
        var dropped = Assert.IsType<UdpDatagramDroppedEvent>(
            droppedTelemetry.Payload);
        Assert.StartsWith("127.0.0.1:", dropped.Remote);
        Assert.Equal(payload.Length, dropped.BytesReceived);
        Assert.Equal("oversized", dropped.Reason);
        Assert.Equal(
            typeof(UdpDatagramDroppedEvent),
            droppedTelemetry.PayloadTypeInfo.Type);
    }

    [Fact]
    public async Task UdpBlockedDatagramTelemetry_DoesNotDelayNextEcho()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(
            HappyEchoEventTypes.UdpDatagramEchoed);

        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0
            });

        try
        {
            byte[] firstPayload = "one"u8.ToArray();

            byte[] firstEcho = await UdpEchoRoundTripAsync(
                IPAddress.Loopback,
                server.UdpPort,
                firstPayload);

            Assert.Equal(firstPayload, firstEcho);

            await missionControl.WaitForAttemptCountAsync(
                HappyEchoEventTypes.UdpDatagramEchoed,
                1,
                ShortTimeout);

            byte[] secondPayload = "two"u8.ToArray();

            byte[] secondEcho = await UdpEchoRoundTripAsync(
                IPAddress.Loopback,
                server.UdpPort,
                secondPayload);

            Assert.Equal(secondPayload, secondEcho);

            await missionControl.WaitForAttemptCountAsync(
                HappyEchoEventTypes.UdpDatagramEchoed,
                2,
                ShortTimeout);
        }
        finally
        {
            missionControl.ReleaseBlockedPublications(
                HappyEchoEventTypes.UdpDatagramEchoed);
        }
    }

    [Fact]
    public async Task UdpBlockedStartedTelemetry_DoesNotDelayEchoTraffic()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.Block(HappyEchoEventTypes.UdpStarted);

        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0
            });

        try
        {
            byte[] payload = "udp-startup-not-blocked"u8.ToArray();

            byte[] echoed = await UdpEchoRoundTripAsync(
                IPAddress.Loopback,
                server.UdpPort,
                payload);

            Assert.Equal(payload, echoed);

            await missionControl.WaitForAttemptAsync(
                HappyEchoEventTypes.UdpStarted,
                ShortTimeout);
        }
        finally
        {
            missionControl.ReleaseBlockedPublications(
                HappyEchoEventTypes.UdpStarted);
        }
    }

    [Fact]
    public async Task UdpTelemetryFailures_DoNotBreakEchoOrShutdown()
    {
        var missionControl = new IntegrationMissionControlClient();
        missionControl.ThrowFor(HappyEchoEventTypes.UdpStarted);
        missionControl.ThrowFor(HappyEchoEventTypes.UdpDatagramDropped);
        missionControl.ThrowFor(HappyEchoEventTypes.UdpDatagramEchoed);
        missionControl.ThrowFor(HappyEchoEventTypes.UdpStopped);

        await using var server = await EchoHost.StartAsync(
            missionControl,
            new HappyEchoOptions
            {
                ListenAddress = "127.0.0.1",
                Port = 0,
                UdpEnabled = true,
                UdpListenAddress = "127.0.0.1",
                UdpPort = 0,
                MaxUdpDatagramBytes = 8
            });

        using var udp = new UdpClient(AddressFamily.InterNetwork);
        udp.Connect(IPAddress.Loopback, server.UdpPort);

        byte[] oversized = new byte[9];
        await udp.SendAsync(oversized, oversized.Length).WaitAsync(ShortTimeout);
        await AssertNoUdpResponseAsync(
            udp,
            TimeSpan.FromMilliseconds(500));

        byte[] firstPayload = [1, 2, 3];
        await udp.SendAsync(firstPayload, firstPayload.Length).WaitAsync(ShortTimeout);
        UdpReceiveResult firstEcho = await udp.ReceiveAsync().WaitAsync(ShortTimeout);
        Assert.Equal(firstPayload, firstEcho.Buffer);

        byte[] secondPayload = [4, 5, 6];
        await udp.SendAsync(secondPayload, secondPayload.Length).WaitAsync(ShortTimeout);
        UdpReceiveResult secondEcho = await udp.ReceiveAsync().WaitAsync(ShortTimeout);
        Assert.Equal(secondPayload, secondEcho.Buffer);

        await server.StopAsync();
        Assert.True(server.Stopped);
        Assert.Contains(
            missionControl.AttemptedPublications,
            e => e.EventType == HappyEchoEventTypes.UdpStopped);
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

    private static async Task<byte[]> UdpEchoRoundTripAsync(
        IPAddress address,
        int port,
        byte[] payload)
    {
        using var udp = new UdpClient(address.AddressFamily);
        udp.Connect(address, port);
        await udp.SendAsync(payload, payload.Length).WaitAsync(ShortTimeout);
        UdpReceiveResult result = await udp.ReceiveAsync().WaitAsync(ShortTimeout);
        return result.Buffer;
    }

    private static async Task AssertNoUdpResponseAsync(
        UdpClient udp,
        TimeSpan timeout)
    {
        using var receiveTimeout = new CancellationTokenSource(timeout);

        try
        {
            UdpReceiveResult unexpected = await udp.ReceiveAsync(
                receiveTimeout.Token);
            Assert.Fail(
                $"Expected no UDP response, but received {unexpected.Buffer.Length} bytes.");
        }
        catch (OperationCanceledException)
        {
        }
        catch (SocketException exception)
            when (exception.SocketErrorCode is
                SocketError.ConnectionReset or
                SocketError.ConnectionRefused)
        {
        }
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

    private static int AllocateTemporaryUdpPort(IPAddress address)
    {
        using var udp = new UdpClient(address.AddressFamily);
        udp.Client.Bind(new IPEndPoint(address, 0));
        return ((IPEndPoint)udp.Client.LocalEndPoint!).Port;
    }

    private sealed class EchoHost : IAsyncDisposable
    {
        private readonly IHost _host;
        private int _stopped;

        private EchoHost(IHost host, int port, int udpPort)
        {
            _host = host;
            Port = port;
            UdpPort = udpPort;
        }

        public int Port { get; }
        public int UdpPort { get; }
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
            int udpPort = testOptions.UdpPort ?? port;
            if (testOptions.UdpEnabled && udpPort == 0)
            {
                string udpListenAddress = string.IsNullOrWhiteSpace(
                    testOptions.UdpListenAddress)
                    ? testOptions.ListenAddress
                    : testOptions.UdpListenAddress;
                udpPort = AllocateTemporaryUdpPort(
                    IPAddress.Parse(udpListenAddress));
            }

            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IMissionControlClient>(missionControl);

            builder.Services.Configure<HappyEchoOptions>(configured =>
            {
                configured.ListenAddress = testOptions.ListenAddress;
                configured.DualMode = testOptions.DualMode;
                configured.Port = port;
                configured.MaxConcurrentConnections = testOptions.MaxConcurrentConnections;
                configured.RequestTimeoutSeconds = testOptions.RequestTimeoutSeconds;
                configured.MaxBytesPerConnection = testOptions.MaxBytesPerConnection;
                configured.TelemetryIgnoredRemoteAddress = testOptions.TelemetryIgnoredRemoteAddress;
                configured.BlockLoopbackConnections = testOptions.BlockLoopbackConnections;
                configured.UdpEnabled = testOptions.UdpEnabled;
                configured.UdpListenAddress = testOptions.UdpListenAddress;
                configured.UdpPort = udpPort;
                configured.MaxUdpDatagramBytes = testOptions.MaxUdpDatagramBytes;
            });

            builder.Services.AddTcpServer<EchoConnectionHandler, HappyEchoOptions>();
            builder.Services.AddHostedService<UdpEchoService>();
            builder.Services.AddHostedService<EchoLifecycleService>();

            IHost host = builder.Build();
            var echoHost = new EchoHost(host, port, udpPort);

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
