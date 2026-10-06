using Orders;

public class OrderRulesTests
{
    static Order O(string product = "book", decimal amount = 10, string customer = "ana") =>
        new(Guid.NewGuid(), customer, product, amount);

    [Fact] public void Valid_order_is_processed() => Assert.Equal(Outcome.Process, OrderRules.Decide(O()).Outcome);
    [Fact] public void Unreadable_payload_goes_to_dlq() => Assert.Equal(Outcome.Invalid, OrderRules.Decide(null).Outcome);
    [Fact] public void Non_positive_amount_goes_to_dlq() => Assert.Equal(Outcome.Invalid, OrderRules.Decide(O(amount: 0)).Outcome);
    [Fact] public void Flaky_is_transient_so_broker_retries() => Assert.Equal(Outcome.Transient, OrderRules.Decide(O("flaky")).Outcome);
    [Fact] public void Flaky_replayed_from_dlq_is_processed() => Assert.Equal(Outcome.Process, OrderRules.Decide(O("flaky"), isReplay: true).Outcome);

    [Fact]
    public void Same_message_twice_is_only_processed_once()
    {
        var store = new IdempotencyStore();
        Assert.True(store.TryStart("m1"));
        Assert.False(store.TryStart("m1"));
    }
}
