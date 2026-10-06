using System.Text.Json;
using Azure.Messaging.ServiceBus;

namespace Orders;

// Consumes the "orders" queue in peek-lock mode: the message stays locked until Complete/Abandon/DeadLetter.
public class OrderConsumer(ServiceBusClient client, IdempotencyStore store, ILogger<OrderConsumer> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var publisher = client.CreateSender(Names.Topic);
        var processor = client.CreateProcessor(Names.Queue, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false, // we complete by hand, only when processing succeeded
            MaxConcurrentCalls = 4,
        });

        processor.ProcessMessageAsync += async args =>
        {
            var msg = args.Message;
            var isReplay = msg.ApplicationProperties.ContainsKey("replay");
            Order? order = null;
            try { order = msg.Body.ToObjectFromJson<Order>(); } catch (JsonException) { }

            var (outcome, reason) = OrderRules.Decide(order, isReplay);
            switch (outcome)
            {
                case Outcome.Invalid:
                    log.LogWarning("DLQ {Id}: {Reason}", msg.MessageId, reason);
                    await args.DeadLetterMessageAsync(msg, "Invalid", reason, ct);
                    return;
                case Outcome.Transient:
                    // Not completed: the exception makes the processor abandon it and the message goes back to the queue.
                    log.LogWarning("Transient failure {Id} (attempt {Count})", msg.MessageId, msg.DeliveryCount);
                    throw new InvalidOperationException(reason);
            }

            // Idempotency: Service Bus delivers "at least once", so the same message can arrive twice.
            if (!store.TryStart(msg.MessageId))
            {
                log.LogInformation("Duplicate ignored {Id}", msg.MessageId);
                await args.CompleteMessageAsync(msg, ct);
                return;
            }

            log.LogInformation("Processed {Id} {Customer} {Amount}", order!.Id, order.Customer, order.Amount);
            var evt = new ServiceBusMessage(BinaryData.FromObjectAsJson(order))
            {
                Subject = "OrderProcessed",
                MessageId = $"{order.Id}-processed",
            };
            evt.ApplicationProperties["amount"] = (double)order.Amount; // the "high-value" SQL filter reads this
            await publisher.SendMessageAsync(evt, ct);
            await args.CompleteMessageAsync(msg, ct);
        };

        processor.ProcessErrorAsync += args =>
        {
            log.LogError(args.Exception, "Error on {Entity}", args.EntityPath);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync(ct);
        await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { });
        await processor.StopProcessingAsync();
    }
}

// Topic "order-events" with 2 subscriptions: each one gets its own copy (pub/sub), with a different filter.
public class SubscriptionListeners(ServiceBusClient client, ILogger<SubscriptionListeners> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var processors = new[] { Names.AllOrders, Names.HighValue }.Select(sub =>
        {
            var p = client.CreateProcessor(Names.Topic, sub);
            p.ProcessMessageAsync += args =>
            {
                log.LogInformation("[{Sub}] received {Id} amount={Amount}", sub, args.Message.MessageId,
                    args.Message.ApplicationProperties["amount"]);
                return Task.CompletedTask; // AutoComplete is true by default
            };
            p.ProcessErrorAsync += args => { log.LogError(args.Exception, "[{Sub}] error", sub); return Task.CompletedTask; };
            return p;
        }).ToList();

        foreach (var p in processors) await p.StartProcessingAsync(ct);
        await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { });
        foreach (var p in processors) await p.StopProcessingAsync();
    }
}

public static class Names
{
    public const string Queue = "orders";
    public const string Topic = "order-events";
    public const string AllOrders = "all-orders";
    public const string HighValue = "high-value";
}
