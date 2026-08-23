using JoyfulReaperLib.TcpServer;

public sealed class HappyEchoOptions : ITcpServerOptions
{
    public const string SectionName = "Echo";

    public string ListenAddress { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 7;
    public int MaxConcurrentConnections { get; set; } = 64;
    public int RequestTimeoutSeconds { get; set; } = 15;
    public long MaxBytesPerConnection { get; set; } = 1_048_576;
    public string? TelemetryIgnoredRemoteAddress { get; set; }
    public bool BlockLoopbackConnections { get; set; }

    public bool UdpEnabled { get; set; } = false;
    public string? UdpListenAddress { get; set; }
    public int? UdpPort { get; set; }
    public int MaxUdpDatagramBytes { get; set; } = 65_507;

    ConnectionLimitBehavior ITcpServerOptions.ConnectionLimitBehavior =>
        ConnectionLimitBehavior.Reject;
}