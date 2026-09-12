# ms-rutaexpress-shipments

Spring Boot microservice for the RutaExpress platform (DUOC "Desarrollo Cloud" project) that owns
shipment lifecycle management: creation, status transitions, and the side effects (Kafka events,
RabbitMQ commands, Catalog capacity checks) triggered by those transitions.

## Tech stack

- Java 21, Spring Boot 4.1.1 (parent POM)
- Spring Web MVC, Spring Data JPA (Oracle via `ojdbc11`), Flyway (`flyway-core` +
  `flyway-database-oracle`)
- Spring Security OAuth2 Resource Server (Azure AD / Entra ID JWT validation)
- Spring for Apache Kafka (`spring-kafka`) — plain JSON strings via `StringSerializer` + Jackson
- Spring AMQP (`spring-boot-starter-amqp`) — RabbitMQ producer
- H2 (test scope only) for the test profile

## Running locally

Requires an Oracle DB reachable (or point `ORACLE_*` vars at your local instance), and — for full
functionality — network access to the Catalog service, Kafka, and RabbitMQ. The app still starts
without Kafka/RabbitMQ actually reachable; publishing is best-effort (logged on failure).

Environment variables (all have local-friendly defaults except the Azure AD ones):

| Variable | Default | Purpose |
|---|---|---|
| `TENANT_ID` | — (required) | Azure AD tenant id, used in the issuer URI |
| `API_CLIENT_ID` | — (required) | Expected JWT audience |
| `ORACLE_HOST` / `ORACLE_PORT` / `ORACLE_SERVICE` | `localhost` / `1521` / `XEPDB1` | Oracle connection |
| `ORACLE_USER` / `ORACLE_PASSWORD` | `rutaexpress` / `rutaexpress` | Oracle credentials |
| `CATALOG_URL` | `http://catalog-svc:8082` | Catalog service base URL |
| `INTERNAL_API_KEY` | `dev-internal-key` | Shared secret sent as `X-Internal-Api-Key` to Catalog's internal endpoint |
| `KAFKA_BOOTSTRAP_SERVERS` | `34.233.170.20:9092` | Shared dev Kafka cluster |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `100.62.195.17` / `5672` | Shared dev RabbitMQ broker |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | `guest` / `guest` | RabbitMQ credentials |

Put these in a `.env` file at the repo root (see `spring.config.import: optional:file:.env[.properties]`)
or export them in your shell. Then:

```bash
./mvnw spring-boot:run
```

The service listens on port **8081** (per `infra/apps/compose.yml`).

## REST API (`/api/shipments`)

All endpoints require a valid Azure AD JWT with one of the roles `Admin`, `Operador`, `Cliente`.

### 1. Create a shipment

```
POST /api/shipments
Content-Type: application/json

{
  "originAddress": "Av. Siempre Viva 123, Santiago",
  "destinationAddress": "Calle Falsa 456, Valparaíso",
  "recipientName": "Juan Pérez",
  "recipientEmail": "juan.perez@example.com",
  "recipientPhone": "+56912345678",
  "serviceId": 4,
  "weightKg": 2.5,
  "declaredValue": 15000
}
```

`201 Created`, header `Location: /api/shipments/{id}`:

```json
{
  "id": 1,
  "originAddress": "Av. Siempre Viva 123, Santiago",
  "destinationAddress": "Calle Falsa 456, Valparaíso",
  "recipientName": "Juan Pérez",
  "recipientEmail": "juan.perez@example.com",
  "recipientPhone": "+56912345678",
  "serviceId": 4,
  "weightKg": 2.5,
  "declaredValue": 15000,
  "status": "CREADO",
  "createdAt": "2026-09-12T15:00:00Z",
  "updatedAt": "2026-09-12T15:00:00Z",
  "version": 0
}
```

### 2. Get a shipment

```
GET /api/shipments/{id}
```

`200 OK` with the same shape as above, or `404`:

```json
{ "error": "SHIPMENT_NOT_FOUND", "message": "Shipment not found: 999" }
```

### 3. Change status

```
PUT /api/shipments/{id}/status
Content-Type: application/json

{ "status": "ACEPTADO" }
```

Order of operations: load shipment (404 if missing) -> validate the transition (409
`INVALID_STATUS_TRANSITION` if illegal) -> if target is `ACEPTADO`, synchronously call Catalog to
decrease capacity (409 `CAPACITY_EXCEEDED` / 502 `UPSTREAM_UNAVAILABLE` abort without changing
status) -> persist (409 `CONCURRENT_UPDATE` on optimistic-lock conflict) -> publish Kafka event ->
publish RabbitMQ command(s), both best-effort -> `200 OK` with the updated resource.

### 4. List / filter shipments

```
GET /api/shipments?status=EN_RUTA&from=2026-09-01&to=2026-09-12&page=0&size=20
```

All query params optional. `from`/`to` are `yyyy-MM-dd`; `from` is inclusive start-of-day UTC, `to`
is inclusive of the whole day (i.e. exclusive of the start of the day *after* `to`). Returns a
Spring Data `Page<ShipmentResponse>`.

## Status transitions

```
CREADO ----> ACEPTADO ----> EN_BODEGA ----> EN_RUTA ----> ENTREGADO (terminal)
  |             |               |
  v             v               v
CANCELADO   CANCELADO       CANCELADO   (all terminal)
```

Any transition not shown above (e.g. `CREADO -> EN_RUTA`, `ACEPTADO -> EN_RUTA`, `EN_RUTA ->
CANCELADO`) is rejected with `409 { "error": "INVALID_STATUS_TRANSITION", "message": "..." }`. The
rules live in `ShipmentStatusTransitionValidator` (pure static transition map, unit tested in
isolation, no Spring context).

## Kafka — `shipments.events`

Published on every successful status change. Key = shipment id (string). Value = JSON string
(Jackson, `StringSerializer` for both key and value — not Spring Kafka's `JsonSerializer` — so any
non-Spring consumer can read it as plain JSON):

```json
{
  "type": "ShipmentStatusChanged",
  "eventId": "b6d8...uuid",
  "timestamp": "2026-09-12T15:05:00Z",
  "traceId": "a1f2...uuid",
  "correlationId": "1",
  "payload": {
    "shipmentId": 1,
    "previousStatus": "CREADO",
    "newStatus": "ACEPTADO",
    "occurredAt": "2026-09-12T15:05:00Z"
  }
}
```

## RabbitMQ — `cmd.direct` exchange

Per the team's shared infra topology (`docs/guia_javier_rutaexpress.md`, section 7 "Topología
RabbitMQ"), this service **only** declares and publishes to the `cmd.direct` exchange (type
`direct`, durable). The other exchanges (`cmd.topic`, `cmd.dead.dlx`) and all queues/DLQs are owned
and declared by `ms-rutaexpress-notify` (the consumer) — declaring an exchange in both places is
idempotent and safe, but queues are deliberately not declared here to keep that ownership boundary
clean.

Status -> routing key mapping (all that apply are published):

| New status | Routing key(s) | Payload |
|---|---|---|
| `ACEPTADO` | `email.send` | `{shipmentId, recipientEmail, recipientName, event:"ACEPTADO", message:"Tu envío ha sido aceptado"}` |
| `EN_BODEGA` | `warehouse.ticket` | `{shipmentId, originAddress, destinationAddress, weightKg}` |
| `EN_RUTA` | `email.send` **and** `label.gen` | email: `event:"EN_RUTA", message:"Tu envío está en ruta"`; label: `{shipmentId, recipientName, destinationAddress, weightKg, serviceId}` |
| `ENTREGADO` | `email.send` | `event:"ENTREGADO", message:"Tu envío ha sido entregado"` |
| `CANCELADO` | *(none)* | Kafka event is still published; no RabbitMQ command |

Each message is the same `EventEnvelope` shape as Kafka, with `type` set to `EmailSendCommand`,
`WarehouseTicketCommand`, or `LabelGenCommand` respectively.

## Design note — synchronous Catalog capacity call

Decreasing capacity on `ACEPTADO` is implemented as a **synchronous** REST call
(`CatalogClient` -> `POST {catalog-url}/api/catalog/services/{serviceId}/decrease-capacity`,
authenticated via `X-Internal-Api-Key`, not a user JWT) rather than an async/eventually-consistent
flow, because capacity is treated as a hard business rule that must be enforced before a shipment
is accepted. **This is pending final confirmation with Javier** — if the team decides an
eventual-consistency approach (e.g. a Kafka/Rabbit command consumed by Catalog with a compensating
rejection event) is preferable, `ShipmentService.changeStatus` and `CatalogClient` are the two
places to revisit.

## Testing

- `ShipmentStatusTransitionValidatorTest` — pure unit test, no Spring context, covers all valid
  transitions plus the required invalid ones (`CREADO->EN_RUTA`, `ACEPTADO->EN_RUTA`,
  `EN_RUTA->CANCELADO`, and both terminal states rejecting every outgoing transition).
- `ShipmentsApplicationTests` — context-load smoke test under the `test` Spring profile (H2
  in-memory DB, Flyway disabled, `ddl-auto: create-drop`).

Run tests:

```bash
./mvnw test
```
