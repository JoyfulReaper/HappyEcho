# HappyEcho

HappyEcho is a lightweight asynchronous TCP echo server written in C# and .NET 10.

It implements the classic Echo Protocol: every byte received from a client is sent back unchanged.

## Features

* Asynchronous TCP connections
* Configurable address and port
* Concurrent connection limit
* Connection timeout
* Maximum bytes per connection
* Pooled network buffers
* Graceful shutdown
* Windows Service support
* Linux `systemd` deployment
* Structured logging

## Requirements

To build HappyEcho:

* .NET 10 SDK

For a framework-dependent Linux deployment:

* .NET 10 runtime
* A Linux x64 VPS
* Permission to accept inbound TCP connections

## Build

Clone the repository:

```bash
git clone https://github.com/JoyfulReaper/HappyEcho.git
cd HappyEcho
```

Build the project:

```bash
dotnet build
```

Run it locally:

```bash
dotnet run --project HappyEcho/HappyEcho.csproj
```

## Configuration

HappyEcho reads settings from the `Echo` configuration section.

```json
{
  "Echo": {
    "ListenAddress": "0.0.0.0",
    "Port": 7,
    "MaxConcurrentConnections": 64,
    "RequestTimeoutSeconds": 15,
    "MaxBytesPerConnection": 1048576
  }
}
```

| Setting                    |     Default | Description                                                                        |
| -------------------------- | ----------: | ---------------------------------------------------------------------------------- |
| `ListenAddress`            | `127.0.0.1` | Address used by the TCP listener. Use `0.0.0.0` to accept remote IPv4 connections. |
| `Port`                     |         `7` | TCP listening port. Port 7 is the traditional Echo Protocol port.                  |
| `MaxConcurrentConnections` |        `64` | Maximum number of simultaneous client connections.                                 |
| `RequestTimeoutSeconds`    |        `15` | Maximum lifetime of one connection.                                                |
| `MaxBytesPerConnection`    |   `1048576` | Maximum bytes echoed during one connection. The default is 1 MiB.                  |

Settings can also be supplied through environment variables:

```bash
Echo__ListenAddress=0.0.0.0
Echo__Port=7
Echo__MaxConcurrentConnections=64
Echo__RequestTimeoutSeconds=15
Echo__MaxBytesPerConnection=1048576
```

> HappyEcho currently rejects loopback clients. For a public VPS deployment, bind to `0.0.0.0` or a specific external address and test it from another machine.

# Linux VPS Deployment

## 1. Publish HappyEcho

From your development machine:

```bash
dotnet publish HappyEcho/HappyEcho.csproj \
  -c Release \
  -r linux-x64 \
  --self-contained false \
  -o publish
```

The published files will be placed in:

```text
publish/
```

## 2. Upload the application

Create an upload directory on the VPS:

```bash
ssh your-user@your-vps \
  "mkdir -p /home/your-user/happyecho-upload"
```

Upload the published files:

```bash
scp -r publish/* \
  your-user@your-vps:/home/your-user/happyecho-upload/
```

Connect to the VPS:

```bash
ssh your-user@your-vps
```

## 3. Create a service account

Create an unprivileged account for HappyEcho:

```bash
sudo useradd \
  --system \
  --home /nonexistent \
  --shell /usr/sbin/nologin \
  happyecho
```

## 4. Install the application

Create the application directory:

```bash
sudo mkdir -p /opt/happyecho
```

Copy the uploaded files:

```bash
sudo cp -a \
  /home/your-user/happyecho-upload/. \
  /opt/happyecho/
```

Keep the application files owned by root:

```bash
sudo chown -R root:root /opt/happyecho
sudo chmod -R a+rX /opt/happyecho
```

Verify that the .NET runtime is installed:

```bash
dotnet --info
```

## 5. Create the environment file

Create the configuration directory:

```bash
sudo mkdir -p /etc/happyecho
```

Create the environment file:

```bash
sudo nano /etc/happyecho/happyecho.env
```

Add:

```dotenv
DOTNET_ENVIRONMENT=Production

Echo__ListenAddress=0.0.0.0
Echo__Port=7
Echo__MaxConcurrentConnections=64
Echo__RequestTimeoutSeconds=15
Echo__MaxBytesPerConnection=1048576
```

Protect it:

```bash
sudo chown root:root /etc/happyecho/happyecho.env
sudo chmod 600 /etc/happyecho/happyecho.env
```

## 6. Create the systemd service

Create:

```bash
sudo nano /etc/systemd/system/happyecho.service
```

Add:

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

# Port 7 is below 1024. This allows the unprivileged service
# account to bind it without running the application as root.
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

Reload systemd:

```bash
sudo systemctl daemon-reload
```

Enable and start HappyEcho:

```bash
sudo systemctl enable --now happyecho
```

Check its status:

```bash
sudo systemctl status happyecho --no-pager
```

## 7. Open the firewall

If UFW is enabled:

```bash
sudo ufw allow 7/tcp
```

Check the firewall:

```bash
sudo ufw status
```

Your VPS provider may also have a separate network firewall. Allow inbound TCP port `7` there as well.

## 8. Confirm the listener

Check that HappyEcho is listening:

```bash
sudo ss -ltnp | grep ':7 '
```

Expected address:

```text
0.0.0.0:7
```

## 9. Test HappyEcho

Because HappyEcho rejects loopback connections, test it from another machine.

Using Netcat:

```bash
printf 'Hello from HappyEcho\n' | nc your-vps-hostname 7
```

Expected output:

```text
Hello from HappyEcho
```

For an interactive connection:

```bash
nc your-vps-hostname 7
```

Anything you type should be echoed back.

You can also test with PowerShell:

```powershell
$client = [System.Net.Sockets.TcpClient]::new(
    "your-vps-hostname",
    7
)

$stream = $client.GetStream()

$data = [Text.Encoding]::UTF8.GetBytes(
    "Hello from PowerShell`n"
)

$stream.Write($data, 0, $data.Length)

$buffer = New-Object byte[] 1024
$count = $stream.Read($buffer, 0, $buffer.Length)

[Text.Encoding]::UTF8.GetString(
    $buffer,
    0,
    $count
)

$client.Dispose()
```

# Service Management

Start HappyEcho:

```bash
sudo systemctl start happyecho
```

Stop it:

```bash
sudo systemctl stop happyecho
```

Restart it:

```bash
sudo systemctl restart happyecho
```

View its status:

```bash
sudo systemctl status happyecho --no-pager
```

Follow logs:

```bash
sudo journalctl -u happyecho -f
```

View recent logs:

```bash
sudo journalctl \
  -u happyecho \
  --since "1 hour ago"
```

Check memory usage:

```bash
systemctl show happyecho \
  -p MemoryCurrent \
  -p MemoryPeak
```

# Updating HappyEcho

Publish and upload the new version, then stop the service:

```bash
sudo systemctl stop happyecho
```

Replace the installed files:

```bash
sudo rm -rf /opt/happyecho/*
sudo cp -a \
  /home/your-user/happyecho-upload/. \
  /opt/happyecho/
```

Restore ownership and permissions:

```bash
sudo chown -R root:root /opt/happyecho
sudo chmod -R a+rX /opt/happyecho
```

Start the service again:

```bash
sudo systemctl start happyecho
```

Check the result:

```bash
sudo systemctl status happyecho --no-pager
sudo journalctl -u happyecho --since "5 minutes ago"
```

# Operational Notes

* HappyEcho is a raw TCP service.
* It does not provide authentication or encryption.
* Public TCP ports will be scanned by automated systems.
* Keep connection, timeout, byte, firewall, and memory limits enabled.
* New connections are immediately closed when all connection slots are occupied.
* `RequestTimeoutSeconds` limits the total connection lifetime. It does not reset after each message.
* `MaxBytesPerConnection` limits the total bytes accepted and echoed by one connection.
* Port 7 is privileged on Linux. Use `CAP_NET_BIND_SERVICE`; do not run HappyEcho as root.

# License

HappyEcho is licensed under the [MIT License](LICENSE).
