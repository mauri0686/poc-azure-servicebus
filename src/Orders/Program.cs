using Azure.Messaging.ServiceBus;
using Orders;

var builder = WebApplication.CreateBuilder(args);

// Local emulator by default (appsettings). On real Azure: the namespace connection string or, better,
// new ServiceBusClient("<ns>.servicebus.windows.net", new DefaultAzureCredential()) with Managed Identity.
builder.Services.AddSingleton(new ServiceBusClient(builder.Configuration["ServiceBus:ConnectionString"]));
builder.Services.AddSingleton<IdempotencyStore>();
builder.Services.AddHostedService<OrderConsumer>();
builder.Services.AddHostedService<SubscriptionListeners>();

var app = builder.Build();

// Publishes an order to the queue. MessageId = order Id: if it is sent twice within the
// duplicate detection window, the broker drops the second copy.
app.MapPost("/orders", async (Order order, ServiceBusClient client) =>
{
    var o = order.Id == Guid.Empty ? order with { Id = Guid.NewGuid() } : order;
    var msg = new ServiceBusMessage(BinaryData.FromObjectAsJson(o))
    {
        MessageId = o.Id.ToString(),
        ContentType = "application/json",
        Subject = "OrderCreated",
    };
    await client.CreateSender(Names.Queue).SendMessageAsync(msg);
    return Results.Accepted($"/orders/{o.Id}", o);
});

// Looks at the dead-letter queue without consuming it (peek).
app.MapGet("/dlq", async (ServiceBusClient client) =>
{
    var receiver = client.CreateReceiver(Names.Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    var msgs = await receiver.PeekMessagesAsync(maxMessages: 20);
    return msgs.Select(m => new { m.MessageId, m.DeadLetterReason, m.DeadLetterErrorDescription, m.DeliveryCount });
});

// Replays the DLQ: reads it, sends a copy flagged as replay back to the queue, and only then completes the original.
app.MapPost("/dlq/replay", async (ServiceBusClient client) =>
{
    var receiver = client.CreateReceiver(Names.Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    var sender = client.CreateSender(Names.Queue);
    var replayed = 0;
    foreach (var m in await receiver.ReceiveMessagesAsync(maxMessages: 20, maxWaitTime: TimeSpan.FromSeconds(2)))
    {
        var copy = new ServiceBusMessage(m) { MessageId = $"{m.MessageId}-replay" }; // new id, otherwise duplicate detection drops it
        copy.ApplicationProperties["replay"] = true;
        await sender.SendMessageAsync(copy);
        await receiver.CompleteMessageAsync(m);
        replayed++;
    }
    return new { replayed };
});

app.Run();
