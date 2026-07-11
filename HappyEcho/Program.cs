/*
 * Happy Echo Server
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */

using HappyEcho;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "Happy Echo Server";
});

builder.Services
    .AddOptions<HappyEchoOptions>()
    .Bind(builder.Configuration.GetSection(HappyEchoOptions.SectionName))
    .Validate(options => options.Port is > 0 and <= 65535, "Echo:Port must be between 1 and 65535.")
    .Validate(options => options.MaxConcurrentConnections > 0, "Echo:MaxConcurrentConnections must be positive.")
    .Validate(options => options.RequestTimeoutSeconds > 0, "Echo:RequestTimeoutSeconds must be positive.")
    .ValidateOnStart();
builder.Services.AddHostedService<EchoWorker>();

var host = builder.Build();
host.Run();
