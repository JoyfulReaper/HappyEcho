using JoyfulReaperLib.MissionControl;
using System.Collections.Concurrent;

namespace HappyEcho.Tests;

public sealed record RecordedMissionControlEvent(
    string EventType,
    object? Payload,
    DateTimeOffset OccurredAt,
    string? CorrelationId);

public sealed class RecordingMissionControlClient : IMissionControlClient
{
    private readonly ConcurrentQueue<RecordedMissionControlEvent> _events = new();
    private readonly SemaphoreSlim _eventSignal = new(0);
    private int _publishedEventCount;

    public IReadOnlyList<RecordedMissionControlEvent> PublishedEvents => _events.ToArray();

    public Func<string, object?, Task>? OnPublishAsync { get; init; }

    public Task<bool> TryPublishAsync<TPayload>(
        string eventType,
        TPayload payload,
        DateTimeOffset occurredAt,
        string? correlationId = null,
        CancellationToken cancellationToken = default)
    {
        _events.Enqueue(new RecordedMissionControlEvent(
            eventType,
            payload,
            occurredAt,
            correlationId));

        Interlocked.Increment(ref _publishedEventCount);
        _eventSignal.Release();

        return OnPublishAsync is null
            ? Task.FromResult(true)
            : CompleteAsync(eventType, payload);
    }

    private async Task<bool> CompleteAsync(
        string eventType,
        object? payload)
    {
        await OnPublishAsync!(eventType, payload);
        return true;
    }

    public async Task WaitForPublishedEventCountAsync(
        int expectedCount,
        CancellationToken cancellationToken = default)
    {
        while (Volatile.Read(ref _publishedEventCount) < expectedCount)
        {
            await _eventSignal.WaitAsync(cancellationToken);
        }
    }
}
