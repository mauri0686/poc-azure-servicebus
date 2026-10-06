using System.Collections.Concurrent;

namespace Orders;

public record Order(Guid Id, string Customer, string Product, decimal Amount);

public enum Outcome { Process, Invalid, Transient }

public static class OrderRules
{
    // Decide qué hacer con un pedido. Puro y testeable: el broker no aparece acá.
    //  - Invalid   -> dead-letter explícito con motivo (no tiene sentido reintentar).
    //  - Transient -> se lanza excepción: Service Bus lo reintenta y, al pasar MaxDeliveryCount, lo manda solo a la DLQ.
    public static (Outcome Outcome, string? Reason) Decide(Order? order, bool isReplay = false)
    {
        if (order is null) return (Outcome.Invalid, "Payload ilegible");
        if (order.Amount <= 0) return (Outcome.Invalid, "Amount debe ser mayor a 0");
        if (string.IsNullOrWhiteSpace(order.Customer)) return (Outcome.Invalid, "Customer vacío");
        // "flaky" simula una dependencia caída (ej. la base). Al reprocesarlo desde la DLQ ya "anda".
        if (order.Product == "flaky" && !isReplay) return (Outcome.Transient, "Dependencia caída (simulada)");
        return (Outcome.Process, null);
    }
}

// ponytail: en memoria, se pierde al reiniciar. En serio: tabla con PK = MessageId (insert falla si ya existe).
public class IdempotencyStore
{
    private readonly ConcurrentDictionary<string, byte> _seen = new();
    public bool TryStart(string messageId) => _seen.TryAdd(messageId, 0);
}
