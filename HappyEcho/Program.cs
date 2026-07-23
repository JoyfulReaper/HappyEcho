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
    .ValidateOnStart();

builder.Services.AddMissionControlClient(
    builder.Configuration.GetSection(
        MissionControlClientOptions.SectionName));

builder.Services.AddTcpServer<EchoConnectionHandler, HappyEchoOptions>();
builder.Services.AddHostedService<EchoLifecycleService>();

var host = builder.Build();
host.Run();
