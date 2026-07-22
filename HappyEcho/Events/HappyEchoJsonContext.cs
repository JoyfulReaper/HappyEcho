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