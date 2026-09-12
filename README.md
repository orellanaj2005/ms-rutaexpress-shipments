# ms-rutaexpress-shipments

Microservicio de la plataforma RutaExpress (proyecto DUOC "Desarrollo Cloud") responsable del
ciclo de vida de los envíos: creación, transiciones de estado, y los efectos secundarios que
disparan esas transiciones (eventos en Kafka, comandos en RabbitMQ, verificación de capacidad
contra Catalog).

## Stack técnico

- Java 21, Spring Boot 4.1.1 (POM padre)
- Spring Web MVC, Spring Data JPA (Oracle vía `ojdbc11`), Flyway (`flyway-core` +
  `flyway-database-oracle`)
- Spring Security OAuth2 Resource Server (validación de JWT de Azure AD / Entra ID)
- Spring for Apache Kafka (`spring-kafka`) — strings JSON planos vía `StringSerializer` + Jackson
- Spring AMQP (`spring-boot-starter-amqp`) — productor RabbitMQ
- H2 (solo en tests) para el perfil `test`

## Cómo correrlo localmente

Necesitas una base Oracle alcanzable (ver el repo hermano `ms-rutaexpress-db` para levantarla en
Docker) y, para funcionalidad completa, acceso de red a Catalog, Kafka y RabbitMQ. El servicio
igual arranca sin Kafka/RabbitMQ disponibles — la publicación es *best-effort* (se registra el
error en el log si falla, no bloquea la respuesta HTTP).

Variables de entorno (todas tienen defaults salvo las de Azure AD):

| Variable | Default | Para qué sirve |
|---|---|---|
| `TENANT_ID` | — (obligatoria) | Tenant id de Azure AD, usado en la issuer URI |
| `API_CLIENT_ID` | — (obligatoria) | Audience esperado del JWT |
| `ORACLE_HOST` / `ORACLE_PORT` / `ORACLE_SERVICE` | `localhost` / `1521` / `XEPDB1` | Conexión Oracle |
| `ORACLE_USER` / `ORACLE_PASSWORD` | `rutaexpress` / `rutaexpress` | Credenciales Oracle |
| `CATALOG_URL` | `http://catalog-svc:8082` | URL base del servicio Catalog |
| `INTERNAL_API_KEY` | `dev-internal-key` | Secreto compartido enviado como `X-Internal-Api-Key` al endpoint interno de Catalog |
| `KAFKA_BOOTSTRAP_SERVERS` | `34.233.170.20:9092` | Cluster Kafka compartido de desarrollo |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `100.62.195.17` / `5672` | Broker RabbitMQ compartido de desarrollo |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | `guest` / `guest` | Credenciales RabbitMQ |

Ponlas en un archivo `.env` en la raíz del repo (ver `spring.config.import:
optional:file:.env[.properties]`) o expórtalas en tu shell. Luego:

```bash
./mvnw spring-boot:run
```

El servicio escucha en el puerto **8081** (según `infra/apps/compose.yml`).

> Si usas la base local de `ms-rutaexpress-db` (Docker), recuerda que su puerto expuesto es
> `1522`, no `1521` — necesitas `ORACLE_PORT=1522` en tu `.env`. Ver el README de ese repo.

## API REST (`/api/shipments`)

Todos los endpoints requieren un JWT válido de Azure AD con alguno de los roles `Admin`,
`Operador`, `Cliente`.

### 1. Crear un envío

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

### 2. Obtener un envío

```
GET /api/shipments/{id}
```

`200 OK` con la misma forma de arriba, o `404`:

```json
{ "error": "SHIPMENT_NOT_FOUND", "message": "Shipment not found: 999" }
```

### 3. Cambiar de estado

```
PUT /api/shipments/{id}/status
Content-Type: application/json

{ "status": "ACEPTADO" }
```

Orden de operaciones: cargar el envío (404 si no existe) → validar la transición (409
`INVALID_STATUS_TRANSITION` si no es válida) → si el destino es `ACEPTADO`, llamar
sincrónicamente a Catalog para decrementar capacidad (409 `CAPACITY_EXCEEDED` / 502
`UPSTREAM_UNAVAILABLE` abortan sin cambiar el estado) → persistir (409 `CONCURRENT_UPDATE` si hay
conflicto de bloqueo optimista) → publicar evento en Kafka → publicar comando(s) en RabbitMQ,
ambos *best-effort* → `200 OK` con el recurso actualizado.

### 4. Listar / filtrar envíos

```
GET /api/shipments?status=EN_RUTA&from=2026-09-01&to=2026-09-12&page=0&size=20
```

Todos los parámetros son opcionales. `from`/`to` son `yyyy-MM-dd`; `from` es inclusivo desde el
inicio del día UTC, `to` incluye el día completo (es decir, excluye el inicio del día *siguiente*
a `to`). Retorna un `Page<ShipmentResponse>` de Spring Data.

## Transiciones de estado

```
CREADO ----> ACEPTADO ----> EN_BODEGA ----> EN_RUTA ----> ENTREGADO (terminal)
  |             |               |
  v             v               v
CANCELADO   CANCELADO       CANCELADO   (todos terminales)
```

Cualquier transición no mostrada arriba (p. ej. `CREADO -> EN_RUTA`, `ACEPTADO -> EN_RUTA`,
`EN_RUTA -> CANCELADO`) se rechaza con `409 { "error": "INVALID_STATUS_TRANSITION", "message":
"..." }`. Las reglas viven en `ShipmentStatusTransitionValidator` (mapa estático de transiciones,
con test unitario aislado, sin contexto de Spring).

## Kafka — `shipments.events`

Se publica en cada cambio de estado exitoso. Key = id del envío (string). Value = string JSON
(Jackson, `StringSerializer` para key y value — no el `JsonSerializer` de Spring Kafka — así
cualquier consumidor no-Spring puede leerlo como JSON plano):

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

## RabbitMQ — exchange `cmd.direct`

Según la topología de infraestructura compartida del equipo (`docs/guia_javier_rutaexpress.md`,
sección 7 "Topología RabbitMQ"), este servicio **solo** declara y publica al exchange
`cmd.direct` (tipo `direct`, durable). Los demás exchanges (`cmd.topic`, `cmd.dead.dlx`) y todas
las colas/DLQ son propiedad y responsabilidad de `ms-rutaexpress-notify` (el consumidor) —
declarar un exchange en ambos lados es idempotente y seguro, pero las colas deliberadamente no se
declaran aquí para mantener esa separación de responsabilidades clara.

Mapeo estado → routing key (se publican todos los que apliquen):

| Estado nuevo | Routing key(s) | Payload |
|---|---|---|
| `ACEPTADO` | `email.send` | `{shipmentId, recipientEmail, recipientName, event:"ACEPTADO", message:"Tu envío ha sido aceptado"}` |
| `EN_BODEGA` | `warehouse.ticket` | `{shipmentId, originAddress, destinationAddress, weightKg}` |
| `EN_RUTA` | `email.send` **y** `label.gen` | email: `event:"EN_RUTA", message:"Tu envío está en ruta"`; label: `{shipmentId, recipientName, destinationAddress, weightKg, serviceId}` |
| `ENTREGADO` | `email.send` | `event:"ENTREGADO", message:"Tu envío ha sido entregado"` |
| `CANCELADO` | *(ninguno)* | El evento Kafka igual se publica; no hay comando RabbitMQ |

Cada mensaje usa el mismo `EventEnvelope` que Kafka, con `type` en `EmailSendCommand`,
`WarehouseTicketCommand` o `LabelGenCommand` según corresponda.

## Decisión de diseño — llamada síncrona a Catalog para capacidad

Decrementar capacidad al pasar a `ACEPTADO` se implementó como llamada REST **síncrona**
(`CatalogClient` → `POST {catalog-url}/api/catalog/services/{serviceId}/decrease-capacity`,
autenticada con `X-Internal-Api-Key`, no con JWT de usuario) en vez de un flujo asíncrono /
eventualmente consistente, porque la capacidad se trata como una regla de negocio dura que debe
aplicarse antes de aceptar un envío. **Esto sigue pendiente de confirmación final con Javier** —
si el equipo decide que conviene más un enfoque eventualmente consistente (p. ej. un comando por
Kafka/Rabbit que Catalog consume, con un evento compensatorio de rechazo), `ShipmentService.changeStatus`
y `CatalogClient` son los dos lugares a revisar.

## Tests

- `ShipmentStatusTransitionValidatorTest` — test unitario puro, sin contexto Spring, cubre todas
  las transiciones válidas más las inválidas requeridas (`CREADO->EN_RUTA`, `ACEPTADO->EN_RUTA`,
  `EN_RUTA->CANCELADO`, y ambos estados terminales rechazando cualquier transición saliente).
- `ShipmentsApplicationTests` — smoke test de carga de contexto bajo el perfil `test` (H2 en
  memoria, Flyway deshabilitado, `ddl-auto: create-drop`).

Correr los tests:

```bash
./mvnw test
```

## Registro de cambios

### 2026-09-12 — Implementación inicial (Jassack)
Se construyó el microservicio completo desde cero según lo pedido: API REST de envíos, máquina
de estados estricta, integración síncrona con Catalog para verificar/decrementar capacidad,
publicación de eventos en Kafka (`shipments.events`) y comandos en RabbitMQ, persistencia en
Oracle vía Flyway. La topología de RabbitMQ se ajustó a mitad de implementación para seguir
exactamente la definida en `docs/guia_javier_rutaexpress.md` (exchange `cmd.direct`, no un
exchange propio inventado), ya que ese documento es la referencia oficial acordada con el equipo.

### 2026-09-12 — Base de datos local en Docker (Jassack)
Se validó el servicio contra una base Oracle real corriendo en Docker (ver
`ms-rutaexpress-db`). El puerto expuesto por ese contenedor es `1522` (no `1521`, porque esta
máquina ya tenía una instalación nativa de Oracle XE ocupando ese puerto), por lo que se agregó
un `.env` local con `ORACLE_PORT=1522` para poder correr las migraciones de Flyway y validar la
tabla `SHIPMENTS`.
