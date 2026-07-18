/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho;

public sealed class HappyEchoOptions
{
    public const string SectionName = "Echo";
    public string ListenAddress { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 7;
    public int MaxConcurrentConnections { get; set; } = 64;
    public int RequestTimeoutSeconds { get; set; } = 15;
    public long MaxBytesPerConnection { get; set; } = 1_048_576;
    public string? TelemetryIgnoredRemoteAddress { get; set; }
    public bool BlockLoopbackConnections { get; set; } = false;
}
