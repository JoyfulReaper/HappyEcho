/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using HappyEcho;
using JoyfulReaperLib.MissionControl;
using JoyfulReaperLib.TcpServer;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "Happy Echo Service";
});

var echoSection = builder.Configuration.GetSection(HappyEchoOptions.SectionName);

builder.Services
    .AddOptions<HappyEchoOptions>()
    .Bind(echoSection)
    .Validate(options => options.Port is > 0 and <= 65535, "Echo:Port must be between 1 and 65535.")
    .Validate(options => options.MaxConcurrentConnections > 0, "Echo:MaxConcurrentConnections must be positive.")
    .Validate(options => options.RequestTimeoutSeconds > 0, "Echo:RequestTimeoutSeconds must be positive.")
    .Validate(options => options.MaxBytesPerConnection > 0, "Echo:MaxBytesPerConnection must be positive.")
    .Validate(options => !options.UdpEnabled ||
        (options.UdpPort ?? options.Port) is > 0 and <= 65535,
            "Echo:UdpPort must be between 1 and 65535 when UDP is enabled.")
    .Validate(
        options => !options.UdpEnabled ||
            options.MaxUdpDatagramBytes is > 0 and <= 65_507,
        "Echo:MaxUdpDatagramBytes must be between 1 and 65507.")
        .ValidateOnStart();

builder.Services.AddMissionControlClient(
    builder.Configuration.GetSection(
        MissionControlClientOptions.SectionName));

builder.Services.AddTcpServer<EchoConnectionHandler, HappyEchoOptions>();
builder.Services.AddHostedService<UdpEchoService>();
builder.Services.AddHostedService<EchoLifecycleService>();

var host = builder.Build();
host.Run();
