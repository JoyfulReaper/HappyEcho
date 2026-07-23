/*
 * Happy Echo Service
 * Copyright (c) 2026 Kyle Givler
 * Licensed under the MIT License.
 */


using HappyEcho.Events;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace HappyEcho;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(StreamingStartedEvent))]
[JsonSerializable(typeof(StreamingStoppedEvent))]
[JsonSerializable(typeof(EchoServiceStartedEvent))]
internal sealed partial class HappyEchoJsonContext
    : JsonSerializerContext;