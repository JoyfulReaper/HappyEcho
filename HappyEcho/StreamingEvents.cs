/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho;

public sealed record StreamingStartedEvent(
    string Remote,
    int RequestTimeoutSeconds,
    long MaxBytesPerConnection);

public sealed record StreamingStoppedEvent(
    string Remote,
    long BytesEchoed,
    long DurationMilliseconds,
    string Outcome,
    bool Succeeded);

