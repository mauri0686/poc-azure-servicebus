using Azure.Messaging.ServiceBus;
using Orders;

var builder = WebApplication.CreateBuilder(args);

// Emulador local por defecto (appsettings). En Azure real: connection string del namespace o, mejor,
// new ServiceBusClient("<ns>.servicebus.windows.net", new DefaultAzureCredential()) con Managed Identity.
builder.Services.AddSingleton(new ServiceBusClient(builder.Configuration["ServiceBus:ConnectionString"]));
builder.Services.AddSingleton<IdempotencyStore>();
builder.Services.AddHostedService<OrderConsumer>();
builder.Services.AddHostedService<SubscriptionListeners>();

var app = builder.Build();

// Publica un pedido en la cola. MessageId = Id del pedido: si se manda dos veces dentro de la ventana de
// duplicate detection, el broker descarta la segunda copia.
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

// Mira la dead-letter queue sin consumirla (peek).
app.MapGet("/dlq", async (ServiceBusClient client) =>
{
    var receiver = client.CreateReceiver(Names.Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    var msgs = await receiver.PeekMessagesAsync(maxMessages: 20);
    return msgs.Select(m => new { m.MessageId, m.DeadLetterReason, m.DeadLetterErrorDescription, m.DeliveryCount });
});

// Reprocesa la DLQ: la lee, reenvía una copia a la cola marcada como replay y recién ahí completa el original.
app.MapPost("/dlq/replay", async (ServiceBusClient client) =>
{
    var receiver = client.CreateReceiver(Names.Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    var sender = client.CreateSender(Names.Queue);
    var replayed = 0;
    foreach (var m in await receiver.ReceiveMessagesAsync(maxMessages: 20, maxWaitTime: TimeSpan.FromSeconds(2)))
    {
        var copy = new ServiceBusMessage(m) { MessageId = $"{m.MessageId}-replay" }; // id nuevo: si no, duplicate detection la descarta
        copy.ApplicationProperties["replay"] = true;
        await sender.SendMessageAsync(copy);
        await receiver.CompleteMessageAsync(m);
        replayed++;
    }
    return new { replayed };
});

app.Run();
