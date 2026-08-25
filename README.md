# HappyEcho

HappyEcho is a lightweight asynchronous TCP and UDP echo server written in C# and .NET 10.

It implements the classic Echo Protocol: every byte received from a client is sent back unchanged. Echoed content is never decoded, stored, logged, or published to telemetry.

## Features

* Asynchronous TCP connections and UDP datagrams
* IPv4, IPv6, and TCP/UDP dual-stack listening
* Configurable address and port
* Concurrent connection limit
* Optional loopback/local-source rejection for loop-attack protection
* Connection timeout
* Maximum bytes per connection
* Pooled network buffers
* Graceful shutdown
* Mission Control streaming lifecycle telemetry
* Production Docker support
* Windows Service support
* Structured logging

# Try it live
Connect to TCP port 7 echo.kgivler.com and send bytes.

## Requirements

To build HappyEcho:

* .NET 10 SDK

For the recommended Linux VPS deployment:

* Docker Engine with Compose
* A Linux VPS with permission to accept inbound TCP connections

## Build And Test

```bash
git clone https://github.com/JoyfulReaper/HappyEcho.git
cd HappyEcho

dotnet restore HappyEcho.slnx
dotnet build HappyEcho.slnx --configuration Release --no-restore
dotnet test HappyEcho.slnx --configuration Release --no-build
```

Run it locally:

```bash
dotnet run --project HappyEcho/HappyEcho.csproj
```

## Local Protocol Testing

Port 7 is the conventional production Echo Protocol port. Use the high,
unprivileged port `7007` for local testing. The following examples work in
Windows PowerShell without `netcat`; run the server command in one terminal and
the matching client command in another.

Define these client helpers once in the client terminal:

```powershell
function Invoke-TcpEcho([Net.IPAddress] $Address, [string] $Text) {
    $client = [Net.Sockets.TcpClient]::new($Address.AddressFamily)
    try {
        $client.Connect($Address, 7007)
        $stream = $client.GetStream()
        $stream.ReadTimeout = 2000
        $payload = [Text.Encoding]::UTF8.GetBytes($Text)
        $stream.Write($payload, 0, $payload.Length)
        $client.Client.Shutdown([Net.Sockets.SocketShutdown]::Send)
        $output = [IO.MemoryStream]::new()
        $stream.CopyTo($output)
        [Text.Encoding]::UTF8.GetString($output.ToArray())
    }
    finally {
        $client.Dispose()
    }
}

function Invoke-UdpEcho([Net.IPAddress] $Address, [string] $Text) {
    $client = [Net.Sockets.UdpClient]::new($Address.AddressFamily)
    try {
        $client.Client.ReceiveTimeout = 2000
        $client.Connect($Address, 7007)
        $payload = [Text.Encoding]::UTF8.GetBytes($Text)
        [void] $client.Send($payload, $payload.Length)
        [Net.IPEndPoint] $remote = $null
        $echoed = $client.Receive([ref] $remote)
        [Text.Encoding]::UTF8.GetString($echoed)
    }
    finally {
        $client.Dispose()
    }
}
```

### TCP IPv4 loopback

Start HappyEcho:

```powershell
$env:Echo__ListenAddress = '127.0.0.1'
$env:Echo__DualMode = 'false'
$env:Echo__Port = '7007'
$env:Echo__UdpEnabled = 'false'
dotnet run --project HappyEcho/HappyEcho.csproj
```

Test it:

```powershell
Invoke-TcpEcho ([Net.IPAddress]::Loopback) 'Hello over TCP IPv4'
```

### TCP IPv6 loopback

Start HappyEcho:

```powershell
$env:Echo__ListenAddress = '::1'
$env:Echo__DualMode = 'false'
$env:Echo__Port = '7007'
$env:Echo__UdpEnabled = 'false'
dotnet run --project HappyEcho/HappyEcho.csproj
```

Test it:

```powershell
Invoke-TcpEcho ([Net.IPAddress]::IPv6Loopback) 'Hello over TCP IPv6'
```

### TCP dual-stack

Start one IPv6 listener that accepts both IPv6 and IPv4 clients:

```powershell
$env:Echo__ListenAddress = '::'
$env:Echo__DualMode = 'true'
$env:Echo__Port = '7007'
$env:Echo__UdpEnabled = 'false'
dotnet run --project HappyEcho/HappyEcho.csproj
```

Test both address families:

```powershell
Invoke-TcpEcho ([Net.IPAddress]::Loopback) 'Hello over dual-stack IPv4'
Invoke-TcpEcho ([Net.IPAddress]::IPv6Loopback) 'Hello over dual-stack IPv6'
```

### UDP IPv4 loopback

Start HappyEcho:

```powershell
$env:Echo__ListenAddress = '127.0.0.1'
$env:Echo__DualMode = 'false'
$env:Echo__Port = '7007'
$env:Echo__UdpEnabled = 'true'
$env:Echo__UdpListenAddress = '127.0.0.1'
$env:Echo__UdpPort = '7007'
dotnet run --project HappyEcho/HappyEcho.csproj
```

Test it:

```powershell
Invoke-UdpEcho ([Net.IPAddress]::Loopback) 'Hello over UDP IPv4'
```

### UDP IPv6 loopback

Start HappyEcho:

```powershell
$env:Echo__ListenAddress = '::1'
$env:Echo__DualMode = 'false'
$env:Echo__Port = '7007'
$env:Echo__UdpEnabled = 'true'
$env:Echo__UdpListenAddress = '::1'
$env:Echo__UdpPort = '7007'
dotnet run --project HappyEcho/HappyEcho.csproj
```

Test it:

```powershell
Invoke-UdpEcho ([Net.IPAddress]::IPv6Loopback) 'Hello over UDP IPv6'
```

TCP Echo returns stream bytes until the client disconnects, the connection
times out, its byte limit is reached, or the server shuts down. UDP Echo returns
each accepted datagram unchanged. UDP support can remain disabled in production
even though the feature is available. Echoed payload content must never be
published to telemetry.

## Configuration

HappyEcho reads settings from the `Echo` configuration section.

```json
{
  "Echo": {
    "ListenAddress": "::",
    "DualMode": true,
    "Port": 7,
    "MaxConcurrentConnections": 64,
    "RequestTimeoutSeconds": 15,
    "MaxBytesPerConnection": 1048576,
    "TelemetryIgnoredRemoteAddress": null,
    "BlockLoopbackConnections": false,
    "UdpEnabled": false,
    "UdpListenAddress": null,
    "UdpPort": null,
    "MaxUdpDatagramBytes": 65507
  },
  "MissionControl": {
    "Enabled": false,
    "BaseUrl": "http://localhost:5190",
    "ApiKey": "",
    "TimeoutMilliseconds": 1000
  }
}
```

| Setting                         |     Default | Description                                                                        |
| ------------------------------- | ----------: | ---------------------------------------------------------------------------------- |
| `ListenAddress`                 |        `::` | Address used by the TCP listener. Use `127.0.0.1` or `::1` for loopback-only testing. |
| `DualMode`                      |      `true` | Allows IPv6 TCP and UDP listeners to accept IPv4 clients too. Dual mode requires the applicable listen address to be `::`. |
| `Port`                          |         `7` | TCP listening port. Port 7 is the traditional Echo Protocol port.                  |
| `MaxConcurrentConnections`      |        `64` | Maximum number of simultaneous client connections.                                 |
| `RequestTimeoutSeconds`         |        `15` | Maximum lifetime of one connection.                                                |
| `MaxBytesPerConnection`         |   `1048576` | Maximum bytes echoed during one connection. The default is 1 MiB.                  |
| `TelemetryIgnoredRemoteAddress` |     `null` | Optional monitor IP whose Echo sessions are processed normally but excluded from Mission Control lifecycle telemetry. |
| `BlockLoopbackConnections`      |    `false` | Optional loop-attack protection. When `true`, loopback/local-source connections are rejected before Echo processing and do not publish streaming-started or streaming-stopped telemetry. |
| `UdpEnabled`                    |    `false` | Enables the UDP Echo listener. It can remain disabled in production.               |
| `UdpListenAddress`              |     `null` | UDP listen address. When unset, the value of `ListenAddress` is used.               |
| `UdpPort`                       |     `null` | UDP listening port. When unset, the value of `Port` is used.                        |
| `MaxUdpDatagramBytes`           |   `65507` | Largest UDP datagram payload that is echoed. Larger datagrams are dropped.          |

Settings can also be supplied through environment variables:

```bash
Echo__ListenAddress=0.0.0.0
Echo__DualMode=false
Echo__Port=7
Echo__MaxConcurrentConnections=64
Echo__RequestTimeoutSeconds=15
Echo__MaxBytesPerConnection=1048576
Echo__TelemetryIgnoredRemoteAddress=172.21.0.1
Echo__BlockLoopbackConnections=false
Echo__UdpEnabled=false
Echo__UdpListenAddress=0.0.0.0
Echo__UdpPort=7
Echo__MaxUdpDatagramBytes=65507

MissionControl__Enabled=true
MissionControl__BaseUrl=http://gateway:8080
MissionControl__ApiKey=replace-with-a-strong-random-key
MissionControl__TimeoutMilliseconds=1000
```

HappyEcho accepts loopback clients by default. Set `BlockLoopbackConnections` to `true` to reject loopback/local-source connections as optional loop-attack protection. When enabled, test the service from another machine or network source.

`TelemetryIgnoredRemoteAddress` suppresses Mission Control telemetry only. The TCP session is still accepted, echoed, timed out, byte-limited, and cleaned up normally. The comparison uses only the normalized remote IP address, not the source port, and IPv4-mapped IPv6 addresses are mapped to IPv4 before comparison. This is intended for Uptime Kuma or another trusted TCP monitor. Docker network gateway addresses vary by host and network, so verify the actual monitor source address before setting it.

## Mission Control Events

HappyEcho publishes best-effort lifecycle telemetry through `JoyfulReaperLib.MissionControl`. Telemetry failures are logged as warnings and never break echo traffic or graceful shutdown.

Event types:

* `happyecho.streaming.started`
* `happyecho.streaming.stopped`

`streaming.started` payload:

* `remote`
* `requestTimeoutSeconds`
* `maxBytesPerConnection`

`streaming.stopped` payload:

* `remote`
* `bytesEchoed`
* `durationMilliseconds`
* `outcome`
* `succeeded`

Outcomes:

| Outcome               | Succeeded | Meaning                                                              |
| --------------------- | --------- | -------------------------------------------------------------------- |
| `client-disconnected` | `true`    | The client completed or disconnected normally before the byte limit. |
| `byte-limit-reached`  | `true`    | HappyEcho successfully enforced `MaxBytesPerConnection`.             |
| `timeout`             | `false`   | The per-connection timeout expired.                                  |
| `io-error`            | `false`   | An `IOException` ended the session.                                  |
| `socket-error`        | `false`   | A `SocketException` ended the session.                               |
| `server-shutdown`     | `false`   | The application stopping token ended the session.                    |
| `failed`              | `false`   | An unexpected exception ended the session.                           |

Started example:

```json
{
  "eventType": "happyecho.streaming.started",
  "payload": {
    "remote": "203.0.113.10:54321",
    "requestTimeoutSeconds": 15,
    "maxBytesPerConnection": 1048576
  }
}
```

Stopped example:

```json
{
  "eventType": "happyecho.streaming.stopped",
  "payload": {
    "remote": "203.0.113.10:54321",
    "bytesEchoed": 21,
    "durationMilliseconds": 4,
    "outcome": "client-disconnected",
    "succeeded": true
  }
}
```

The two events for one echo session share the same Mission Control correlation ID.

## Docker

Build the image:

```bash
docker build --no-cache -t happy-echo .
```

The Dockerfile:

* Restores, tests, and publishes in a .NET SDK build stage.
* Publishes a Native AOT executable.
* Uses the small .NET `runtime-deps` image for the final stage.
* Does not contain the full managed .NET runtime.
* Runs as `${APP_UID}`, not root.
* Listens internally on TCP port `7007`.
* Can be published externally as canonical Echo TCP port `7` with `7:7007`.
* Does not embed configuration or secrets.

The Docker image defaults to:

```dockerfile
ENV Echo__ListenAddress=0.0.0.0
ENV Echo__Port=7007
```

Local unprivileged mapping:

```bash
docker run --rm -p 7007:7007 happy-echo
```

Canonical public Echo mapping:

```bash
docker run --rm -p 7:7007 happy-echo
```

Publishing host port 7 may require host-level privileges on Linux and macOS,
while the process inside the container remains non-root and needs no added
Linux capability.

No Docker health check is currently defined. Loopback connections are accepted by default, so an in-container loopback health check would be rejected only when `BlockLoopbackConnections=true`. The current `runtime-deps` image does not include a TCP probing utility such as `nc`. Compose can still verify the service process state, and listener checks should be performed externally or from the host.

## Linux VPS Deployment With Docker Compose

Docker Compose is the recommended Linux deployment path.

### 1. Clone Or Update The Repository

```bash
cd /opt/joyful-stack

git clone https://github.com/JoyfulReaper/HappyEcho.git HappyEcho
```

For updates:

```bash
cd /opt/joyful-stack/HappyEcho
git pull
```

### 2. Add The Mission Control Source Key

Add a source entry to the Mission Control gateway configuration:

```yaml
EventSources__Sources__4__Name: happyecho-production
EventSources__Sources__4__ApiKey: ${HAPPYECHO_MISSION_CONTROL_KEY}
```

Add the required value to `/opt/joyful-stack/.env`:

```dotenv
HAPPYECHO_MISSION_CONTROL_KEY=replace-with-a-strong-random-key
```

Do not commit real secrets.

### 3. Add The Compose Service

Add this service to `/opt/joyful-stack/compose.yaml`:

```yaml
happyecho:
  build:
    context: ./HappyEcho
    dockerfile: Dockerfile

  restart: unless-stopped
  init: true

  environment:
    DOTNET_ENVIRONMENT: Production

    Echo__ListenAddress: 0.0.0.0
    Echo__DualMode: "false"
    Echo__Port: 7007
    Echo__MaxConcurrentConnections: 64
    Echo__RequestTimeoutSeconds: 15
    Echo__MaxBytesPerConnection: 1048576
    Echo__TelemetryIgnoredRemoteAddress: "172.21.0.1"
    Echo__BlockLoopbackConnections: "false"

    MissionControl__Enabled: "true"
    MissionControl__BaseUrl: http://gateway:8080
    MissionControl__ApiKey: ${HAPPYECHO_MISSION_CONTROL_KEY}
    MissionControl__TimeoutMilliseconds: 1000

  ports:
    - "7:7007/tcp"

  cap_drop:
    - ALL

  security_opt:
    - no-new-privileges:true

  depends_on:
    gateway:
      condition: service_started

  networks:
    - backend
```

### 4. Validate Compose

```bash
cd /opt/joyful-stack

docker compose config --quiet
```

### 5. Build HappyEcho

```bash
docker compose build \
  --no-cache \
  --progress=plain \
  happyecho
```

### 6. Stop The Old systemd Service

```bash
sudo systemctl disable --now happyecho.service
```

### 7. Start The Container

```bash
docker compose up \
  -d \
  happyecho
```

### 8. Verify The TCP Listener

```bash
docker compose ps happyecho

docker compose logs \
  --tail=200 \
  happyecho

sudo ss -ltnp | grep ':7 '
```

### 9. Test From An External Machine

```bash
printf 'Hello from HappyEcho\n' | nc your-vps-hostname 7
```

Expected output:

```text
Hello from HappyEcho
```

### 10. Confirm Mission Control Events

Mission Control should contain a matching pair with the same correlation ID:

```text
happyecho.streaming.started
happyecho.streaming.stopped
```

## Legacy systemd Rollback

Use this path only if Docker Compose needs to be rolled back.

Publish HappyEcho:

```bash
dotnet publish HappyEcho/HappyEcho.csproj \
  -c Release \
  -r linux-x64 \
  --self-contained false \
  -o publish
```

Install under `/opt/happyecho`, run it as an unprivileged `happyecho` user, and grant only `CAP_NET_BIND_SERVICE` so port 7 can bind without root.

Example service:

```ini
[Unit]
Description=HappyEcho TCP Echo Server
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=happyecho
Group=happyecho
WorkingDirectory=/opt/happyecho
ExecStart=/usr/bin/dotnet /opt/happyecho/HappyEcho.dll
EnvironmentFile=/etc/happyecho/happyecho.env
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
MemoryMax=128M

[Install]
WantedBy=multi-user.target
```

## Operational Notes

* HappyEcho is a raw TCP service.
* It does not provide authentication or encryption.
* Public TCP ports will be scanned by automated systems.
* Keep connection, timeout, byte, firewall, and memory limits enabled.
* New connections are immediately closed when all connection slots are occupied.
* `RequestTimeoutSeconds` limits the total connection lifetime. It does not reset after each message.
* `MaxBytesPerConnection` limits the total bytes accepted and echoed by one connection.
* Port 7 is privileged on Linux. Publish host port 7 to container port 7007; do not run HappyEcho as root and do not add `NET_BIND_SERVICE` to the container.
* Loopback connections are accepted by default.
* When `BlockLoopbackConnections` is enabled, rejected loopback/local-source connections produce no streaming lifecycle telemetry because rejection happens before streaming begins.

## License

HappyEcho is licensed under the [MIT License](LICENSE).
