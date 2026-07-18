/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho.Events;

public sealed record EchoServiceStartedEvent(
    string ListenAddress);