/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using JoyfulReaperLib.JRNet;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Sockets;

namespace HappyEcho;

public sealed class UdpEchoService(
    ILogger<UdpEchoService> logger,
    IOptions<HappyEchoOptions> options)
    : BackgroundService
{
    private const int MaximumUdpPayloadBytes = 65_507;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        HappyEchoOptions value = options.Value;

        if (!value.UdpEnabled)
        {
            logger.LogInformation("HappyEcho UDP listener disabled.");
            return;
        }

        IPAddress listenAddress = IPAddressUtils.ParseListenAddress(
            string.IsNullOrWhiteSpace(value.UdpListenAddress)
                ? value.ListenAddress
                : value.UdpListenAddress);

        int port = value.UdpPort ?? value.Port;
        int maxDatagramBytes = Math.Clamp(
            value.MaxUdpDatagramBytes,
            1,
            MaximumUdpPayloadBytes);

        using UdpClient udp = CreateUdpClient(listenAddress, port);

        logger.LogInformation(
            "HappyEcho UDP listener started on {Endpoint}",
            udp.Client.LocalEndPoint);

        while (!stoppingToken.IsCancellationRequested)
        {
            UdpReceiveResult received;

            try
            {
                received = await udp.ReceiveAsync(stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (SocketException exception)
            {
                logger.LogWarning(
                    exception,
                    "Socket error while receiving UDP Echo datagram.");

                continue;
            }
            catch (ObjectDisposedException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            if (ShouldBlockDatagram(received.RemoteEndPoint, listenAddress, value))
            {
                logger.LogWarning(
                    "[SECURITY] Dropped UDP loopback datagram from {Remote}",
                    received.RemoteEndPoint);

                continue;
            }

            if (received.Buffer.Length > maxDatagramBytes)
            {
                logger.LogWarning(
                    "Dropped oversized UDP Echo datagram from {Remote}: {Bytes} bytes.",
                    received.RemoteEndPoint,
                    received.Buffer.Length);

                continue;
            }

            try
            {
                await udp.SendAsync(
                    received.Buffer,
                    received.Buffer.Length,
                    received.RemoteEndPoint);

                logger.LogDebug(
                    "Echoed UDP datagram for {Remote}: {Bytes} bytes.",
                    received.RemoteEndPoint,
                    received.Buffer.Length);
            }
            catch (SocketException exception)
            {
                logger.LogWarning(
                    exception,
                    "Socket error while sending UDP Echo datagram to {Remote}.",
                    received.RemoteEndPoint);
            }
        }

        logger.LogInformation("HappyEcho UDP listener stopped.");
    }

    private static UdpClient CreateUdpClient(IPAddress address, int port)
    {
        var udp = new UdpClient(address.AddressFamily);

        if (address.AddressFamily == AddressFamily.InterNetworkV6 &&
            address.Equals(IPAddress.IPv6Any))
        {
            udp.Client.DualMode = true;
        }

        udp.Client.Bind(new IPEndPoint(address, port));

        return udp;
    }

    private static bool ShouldBlockDatagram(
        IPEndPoint remote,
        IPAddress configuredListenAddress,
        HappyEchoOptions options)
    {
        if (!options.BlockLoopbackConnections)
        {
            return false;
        }

        return IPAddress.IsLoopback(remote.Address) ||
            remote.Address.Equals(configuredListenAddress);
    }
}