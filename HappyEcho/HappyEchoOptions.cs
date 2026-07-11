/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho;

public sealed class HappyEchoOptions
{
    public const string SectionName = "Echo";
    public string ListenAddress { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 7;
    public int MaxConcurrentConnections { get; init; } = 64;
    public int RequestTimeoutSeconds { get; init; } = 15;
}
