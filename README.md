# POC Azure Service Bus: pedidos con cola, topic, reintentos y dead-letter

POC personal para cerrar la brecha "Azure Service Bus" (rechazos de Nir Yu y CaseWorthy). Corre local
con el **emulador oficial** de Service Bus, sin gastar Azure.

> Honestidad en entrevistas: esto **no es experiencia en producción**. La frase es:
> *"No lo usé en producción, pero armé un POC con una API que publica en una cola de Service Bus, un
> consumer con peek-lock, reintentos, dead-letter e idempotencia, y un topic con dos subscriptions
> filtradas. El patrón lo conozco de DDD y de idempotencia en pagos."*

## Qué demuestra

| Concepto | Dónde |
|---|---|
| Cola con **peek-lock** y `Complete` manual | `OrderConsumer` (`AutoCompleteMessages = false`) |
| **Reintentos** y **dead-letter automática** al pasar `MaxDeliveryCount` (3) | producto `flaky` |
| **Dead-letter explícito** con motivo (sin reintentar lo que nunca va a andar) | `amount <= 0` |
| **Duplicate detection** del broker (`MessageId` = id del pedido, ventana 1 min) | mismo id dos veces |
| **Idempotencia del consumer** (Service Bus es *al menos una vez*) | `IdempotencyStore` |
| **Topic + 2 subscriptions** (pub/sub), una con **SQL filter** `amount > 1000` | `SubscriptionListeners` + `emulator/Config.json` |
| **Reprocesar la DLQ** | `POST /dlq/replay` |

## Cómo correrlo

1. Instalar **Docker Desktop** (hoy no está en esta PC).
2. `docker compose up -d` (levanta el emulador + Azure SQL Edge; la primera vez tarda).
3. `dotnet run --project src/Orders`
4. Abrir `demo.http` en Rider o VS Code y ejecutar los pasos en orden, mirando la consola.
5. Tests de la lógica (no necesitan Docker): `dotnet test`

Contra Azure real: crear un namespace **Standard** (los topics no existen en Basic), crear la cola y el
topic con las mismas propiedades de `emulator/Config.json` y poner la connection string en
`ServiceBus:ConnectionString`. Borrar el resource group al terminar.

## Lo que te van a preguntar

- **Cola vs topic**: en la cola cada mensaje lo procesa UN consumidor (competing consumers). En el topic cada subscription recibe su copia (pub/sub) y puede filtrar.
- **Peek-lock vs receive-and-delete**: peek-lock bloquea el mensaje durante el `LockDuration`; si no hacés `Complete` vuelve a la cola. Receive-and-delete lo borra al leerlo: más rápido, pero si el proceso se cae se pierde.
- **Dead-letter queue**: sub-cola donde caen los mensajes que superan `MaxDeliveryCount`, que expiran, o que el código manda a propósito con un motivo. Se monitorea y se reprocesa.
- **Duplicate detection vs idempotencia**: el broker descarta el mismo `MessageId` dentro de una ventana, pero la entrega es *al menos una vez* (un lock que vence reentrega). Por eso el consumer igual tiene que ser idempotente.
- **Sessions**: orden FIFO garantizado por clave (por ejemplo, todos los mensajes de un mismo pedido).
- **Service Bus vs Storage Queues vs Event Grid vs Event Hubs**: Service Bus = mensajería de negocio con garantías (DLQ, sessions, transacciones). Storage Queue = cola simple y barata. Event Grid = notificación reactiva de que algo pasó. Event Hubs = streaming de alto volumen (telemetría, tipo Kafka).
- **Seguridad**: en Azure, sin connection strings: `ServiceBusClient` con `DefaultAzureCredential` y **Managed Identity** con rol *Azure Service Bus Data Sender/Receiver*.

## Próximos pasos (POC 1 completo, ver `interview-prep/brechas-rechazos-pocs.md`)

- Reemplazar el `BackgroundService` por una **Azure Function** con trigger de Service Bus (isolated worker).
- Secretos en **Key Vault** con Managed Identity, deploy con **Azure DevOps** y logs en Application Insights.
