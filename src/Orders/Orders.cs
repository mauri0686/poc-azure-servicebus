using System.Collections.Concurrent;

namespace Orders;

public record Order(Guid Id, string Customer, string Product, decimal Amount);

public enum Outcome { Process, Invalid, Transient }

public static class OrderRules
{
    // Decides what to do with an order. Pure and testable: the broker does not show up here.
    //  - Invalid   -> explicit dead-letter with a reason (retrying would never help).
    //  - Transient -> throw: Service Bus retries it and, past MaxDeliveryCount, moves it to the DLQ on its own.
    public static (Outcome Outcome, string? Reason) Decide(Order? order, bool isReplay = false)
    {
        if (order is null) return (Outcome.Invalid, "Unreadable payload");
        if (order.Amount <= 0) return (Outcome.Invalid, "Amount must be greater than 0");
        if (string.IsNullOrWhiteSpace(order.Customer)) return (Outcome.Invalid, "Customer is empty");
        // "flaky" simulates a dependency that is down (e.g. the database). Replayed from the DLQ, it works.
        if (order.Product == "flaky" && !isReplay) return (Outcome.Transient, "Dependency unavailable (simulated)");
        return (Outcome.Process, null);
    }
}

// ponytail: in memory, lost on restart. For real: a table with PK = MessageId (the insert fails if it already exists).
public class IdempotencyStore
{
    private readonly ConcurrentDictionary<string, byte> _seen = new();
    public bool TryStart(string messageId) => _seen.TryAdd(messageId, 0);
}
