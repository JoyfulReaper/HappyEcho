/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho;

internal sealed class EchoSessionState
{
    public long BytesEchoed { get; set; }
    public bool ByteLimitReached { get; set; }
}
