/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

namespace HappyEcho.Events;

public static class HappyEchoEventTypes
{
    public const string StreamingStarted =
        "happyecho.streaming.started";

    public const string StreamingStopped =
        "happyecho.streaming.stopped";

    public const string ServiceStarted =
        "happyecho.service.started";

    public const string UdpStarted =
        "happyecho.udp.started";

    public const string UdpStopped =
        "happyecho.udp.stopped";

    public const string UdpDatagramEchoed =
        "happyecho.udp.datagram.echoed";

    public const string UdpDatagramDropped =
        "happyecho.udp.datagram.dropped";
}
