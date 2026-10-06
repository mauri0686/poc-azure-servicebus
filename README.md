# Azure Service Bus POC: orders with a queue, a topic, retries and dead-lettering

A small .NET 8 proof of concept that exercises the core Azure Service Bus patterns end to end. It runs
locally against the **official Service Bus emulator**, so it costs nothing in Azure.

## What it demonstrates

| Concept | Where |
|---|---|
| Queue in **peek-lock** mode with manual `Complete` | `OrderConsumer` (`AutoCompleteMessages = false`) |
| **Retries** and **automatic dead-lettering** after `MaxDeliveryCount` (3) | product `flaky` |
| **Explicit dead-lettering** with a reason (no point retrying what will never work) | `amount <= 0` |
| Broker-side **duplicate detection** (`MessageId` = order id, 1-minute window) | same id sent twice |
| **Idempotent consumer** (Service Bus delivers *at least once*) | `IdempotencyStore` |
| **Topic + 2 subscriptions** (pub/sub), one with a **SQL filter** `amount > 1000` | `SubscriptionListeners` + `emulator/Config.json` |
| **Replaying the DLQ** | `POST /dlq/replay` |

## How to run it

1. Install **Docker Desktop**.
2. `docker compose up -d` (starts the emulator plus Azure SQL Edge; the first run takes a while).
3. `dotnet run --project src/Orders`
4. Open `demo.http` in Rider or VS Code and run the requests in order while watching the console.
5. Unit tests for the decision logic (no Docker needed): `dotnet test`

Against real Azure: create a **Standard** namespace (Basic has no topics), create the queue and the topic
with the same properties as `emulator/Config.json`, and set `ServiceBus:ConnectionString`. Delete the
resource group when you are done.

## Key concepts

- **Queue vs topic**: in a queue each message is processed by ONE consumer (competing consumers). In a topic each subscription gets its own copy (pub/sub) and can filter.
- **Peek-lock vs receive-and-delete**: peek-lock locks the message for `LockDuration`; if it is not completed, it goes back to the queue. Receive-and-delete removes it on read: faster, but the message is lost if the process crashes.
- **Dead-letter queue**: a sub-queue for messages that exceed `MaxDeliveryCount`, expire, or are dead-lettered on purpose with a reason. It must be monitored and replayed.
- **Duplicate detection vs idempotency**: the broker drops the same `MessageId` within a time window, but delivery is still *at least once* (an expired lock means redelivery), so the consumer must be idempotent anyway.
- **Sessions**: guaranteed FIFO ordering per key (for example, all messages for the same order).
- **Service Bus vs Storage Queues vs Event Grid vs Event Hubs**: Service Bus is business messaging with guarantees (DLQ, sessions, transactions). Storage Queues are a simple, cheap queue. Event Grid is reactive notification that something happened. Event Hubs is high-volume streaming (telemetry, Kafka-like).
- **Security**: in Azure, no connection strings: `ServiceBusClient` with `DefaultAzureCredential` and a **Managed Identity** holding the *Azure Service Bus Data Sender/Receiver* role.

## Next steps

- Replace the `BackgroundService` consumer with an **Azure Function** using a Service Bus trigger (isolated worker).
- Secrets in **Key Vault** via Managed Identity, deployment through an **Azure DevOps** pipeline, logs in Application Insights.
