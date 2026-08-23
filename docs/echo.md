# Echo Protocol Compliance

Protocol: Echo
RFC: RFC 862
Conventional port: 7

## Supported transports

- TCP
- UDP

## TCP behavior

- Accepts stream connections.
- Echoes received bytes back unchanged.
- Continues until client disconnects, timeout, byte limit, or server shutdown.
- Supports IPv4, IPv6, and optional dual-stack mode.

## UDP behavior

- Receives one datagram.
- Sends the exact datagram payload back to the sender.
- No session state.
- Supports IPv4 and IPv6.
- May be disabled in production.

## Intentional limits/deviations

- Local/development examples use port 7007 instead of privileged port 7.
- TCP sessions have configurable timeout and byte limit.
- Oversized UDP datagrams may be dropped according to MaxUdpDatagramBytes.
- Payload content is not published to telemetry.

## Manual verification

See README local protocol testing section.