/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho.Events;

public sealed record UdpEchoStartedEvent(
    string ListenEndpoint,
    int MaxDatagramBytes,
    bool BlockLoopbackConnections);

public sealed record UdpEchoStoppedEvent(
    string ListenEndpoint,
    long DatagramsReceived,
    long DatagramsEchoed,
    long DatagramsDropped,
    long BytesEchoed,
    long DurationMilliseconds);

public sealed record UdpDatagramEchoedEvent(
    string Remote,
    int BytesEchoed);

public sealed record UdpDatagramDroppedEvent(
    string Remote,
    int BytesReceived,
    string Reason);
