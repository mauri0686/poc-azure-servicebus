using System.Text.Json;
using Azure.Messaging.ServiceBus;

namespace Orders;

// Consume la cola "orders" en modo peek-lock: el mensaje queda bloqueado hasta Complete/Abandon/DeadLetter.
public class OrderConsumer(ServiceBusClient client, IdempotencyStore store, ILogger<OrderConsumer> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var publisher = client.CreateSender(Names.Topic);
        var processor = client.CreateProcessor(Names.Queue, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false, // completamos a mano, solo si salió bien
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
                    // No completamos: la excepción hace que el processor haga Abandon y el mensaje vuelva a la cola.
                    log.LogWarning("Falla transitoria {Id} (intento {Count})", msg.MessageId, msg.DeliveryCount);
                    throw new InvalidOperationException(reason);
            }

            // Idempotencia: Service Bus garantiza "al menos una vez", así que el mismo mensaje puede llegar 2 veces.
            if (!store.TryStart(msg.MessageId))
            {
                log.LogInformation("Duplicado ignorado {Id}", msg.MessageId);
                await args.CompleteMessageAsync(msg, ct);
                return;
            }

            log.LogInformation("Procesado {Id} {Customer} {Amount}", order!.Id, order.Customer, order.Amount);
            var evt = new ServiceBusMessage(BinaryData.FromObjectAsJson(order))
            {
                Subject = "OrderProcessed",
                MessageId = $"{order.Id}-processed",
            };
            evt.ApplicationProperties["amount"] = (double)order.Amount; // la SQL filter de "high-value" lee esto
            await publisher.SendMessageAsync(evt, ct);
            await args.CompleteMessageAsync(msg, ct);
        };

        processor.ProcessErrorAsync += args =>
        {
            log.LogError(args.Exception, "Error en {Entity}", args.EntityPath);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync(ct);
        await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { });
        await processor.StopProcessingAsync();
    }
}

// Topic "order-events" con 2 subscriptions: cada una recibe SU copia (pub/sub), con filtro distinto.
public class SubscriptionListeners(ServiceBusClient client, ILogger<SubscriptionListeners> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var processors = new[] { Names.AllOrders, Names.HighValue }.Select(sub =>
        {
            var p = client.CreateProcessor(Names.Topic, sub);
            p.ProcessMessageAsync += args =>
            {
                log.LogInformation("[{Sub}] recibió {Id} amount={Amount}", sub, args.Message.MessageId,
                    args.Message.ApplicationProperties["amount"]);
                return Task.CompletedTask; // AutoComplete=true por defecto
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
