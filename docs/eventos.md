# Contrato de eventos Kafka de RutaExpress

Responsable: Jassack Mendoza (dueño del productor, ms-rutaexpress-shipments).
Revisores: Valentina Morales (audit, report) y Javier Orellana (infraestructura, kafka-admin).

Cualquier cambio a este formato se hace primero en este archivo y se avisa al equipo antes de
tocar código. Los consumidores deben ignorar los campos que no conocen
(`@JsonIgnoreProperties(ignoreUnknown = true)`), así agregar un campo no rompe a nadie.

## 1. Sobre común (EventEnvelope)

| Campo           | Tipo                 | Uso |
|-----------------|----------------------|-----|
| `eventId`       | UUID (texto)         | Identifica el evento. Los consumidores lo usan para descartar duplicados cuando Kafka reentrega un mensaje. |
| `type`          | String               | `ShipmentCreated`, `ShipmentStatusChanged` o `AuditAction`. Cada consumidor decide qué procesa. |
| `version`       | int                  | Versión del esquema del payload. Hoy es `1` para los tres tipos. |
| `aggregateId`   | String               | Id del envío. Es también la clave del mensaje: todos los eventos de un envío caen en la misma partición y se leen en orden. |
| `timestamp`     | Instant (ISO-8601 UTC) | Momento en que ocurrió el hecho. Es la fecha que muestra el timeline. |
| `traceId`       | String               | Mismo valor para todos los eventos que nacen de una misma operación HTTP (por ejemplo, el `ShipmentStatusChanged` y su `AuditAction`). |
| `correlationId` | String               | Id del envío, para agrupar en los logs todo lo que pasó con él. |
| `producer`      | String               | Microservicio que emitió el evento (`ms-rutaexpress-shipments`). |
| `payload`       | Objeto               | Datos propios de cada tipo (sección 2). |

Reglas del productor:

- Clave del mensaje: `shipmentId` como texto.
- Valor: JSON sin cabeceras de tipo de Spring (`__TypeId__`), para que cualquier consumidor lo lea
  sin depender de clases Java del productor.
- Los eventos se publican después del commit de la transacción: si el cambio se revierte, no sale
  ningún evento.
- Las fechas van en ISO-8601 UTC (`2026-11-04T14:03:12.481Z`).

## 2. Payloads

### 2.1 ShipmentCreated (tópico `shipments.events`)

| Campo         | Tipo   | Descripción |
|---------------|--------|-------------|
| `shipmentId`  | long   | Id del envío. |
| `serviceId`   | long   | Id del servicio de envío en catalog. |
| `serviceType` | String | Tipo de servicio usado por report para agrupar. Ver nota al final. |
| `status`      | String | Siempre `CREADO`. |
| `origin`      | String | Dirección de origen. |
| `destination` | String | Dirección de destino. |
| `createdBy`   | String | Usuario que creó el envío (claim `preferred_username` del JWT). |

```json
{
  "eventId": "6f1c2a7e-6c0b-4d55-9a57-3e1d8a0b9f21",
  "type": "ShipmentCreated",
  "version": 1,
  "aggregateId": "42",
  "timestamp": "2026-11-04T14:03:12.481Z",
  "traceId": "0b7f5a2c-9d1e-4f0a-8c3b-2a6e1d4c7f90",
  "correlationId": "42",
  "producer": "ms-rutaexpress-shipments",
  "payload": {
    "shipmentId": 42,
    "serviceId": 3,
    "serviceType": "3",
    "status": "CREADO",
    "origin": "Av. Providencia 1234, Santiago",
    "destination": "Calle Prat 55, Valparaíso",
    "createdBy": "cliente@rutaexpress.cl"
  }
}
```

### 2.2 ShipmentStatusChanged (tópico `shipments.events`)

| Campo            | Tipo    | Descripción |
|------------------|---------|-------------|
| `shipmentId`     | long    | Id del envío. |
| `previousStatus` | String  | Estado anterior. |
| `status`         | String  | Estado nuevo. Se llama `status`, no `newStatus`. |
| `serviceType`    | String  | Igual que en `ShipmentCreated`. |
| `changedBy`      | String  | Usuario que hizo el cambio. |
| `changedAt`      | Instant | Momento del cambio. |
| `recipientEmail` | String  | Correo del destinatario. Lo usa notify para armar el comando de email. |
| `recipientName`  | String  | Nombre del destinatario, para el mismo fin. |

```json
{
  "eventId": "a3d9e1b4-2f7c-4c1a-bb0e-7d5f6e8c9a12",
  "type": "ShipmentStatusChanged",
  "version": 1,
  "aggregateId": "42",
  "timestamp": "2026-11-05T09:15:40.002Z",
  "traceId": "c41e8f3a-7b2d-4e6f-9a1c-5d0b3e7f2a84",
  "correlationId": "42",
  "producer": "ms-rutaexpress-shipments",
  "payload": {
    "shipmentId": 42,
    "previousStatus": "EN_BODEGA",
    "status": "EN_RUTA",
    "serviceType": "3",
    "changedBy": "operador@rutaexpress.cl",
    "changedAt": "2026-11-05T09:15:40.002Z",
    "recipientEmail": "ana.perez@correo.cl",
    "recipientName": "Ana Pérez"
  }
}
```

### 2.3 AuditAction (tópico `audit.timeline`)

| Campo        | Tipo    | Descripción |
|--------------|---------|-------------|
| `shipmentId` | long    | Envío sobre el que se actuó. |
| `actor`      | String  | Usuario que hizo la acción. |
| `role`       | String  | Rol del usuario (`Admin`, `Operador` o `Cliente`, claim `roles` del JWT). |
| `action`     | String  | `CREAR_ENVIO`, `CAMBIAR_ESTADO` o `CANCELAR_ENVIO`. |
| `details`    | String  | Texto legible para el timeline. |
| `occurredAt` | Instant | Momento de la acción. |

```json
{
  "eventId": "e7b2c4d6-1a3f-4b5e-8c7d-9f0a1b2c3d4e",
  "type": "AuditAction",
  "version": 1,
  "aggregateId": "42",
  "timestamp": "2026-11-05T09:15:40.002Z",
  "traceId": "c41e8f3a-7b2d-4e6f-9a1c-5d0b3e7f2a84",
  "correlationId": "42",
  "producer": "ms-rutaexpress-shipments",
  "payload": {
    "shipmentId": 42,
    "actor": "operador@rutaexpress.cl",
    "role": "Operador",
    "action": "CAMBIAR_ESTADO",
    "details": "Estado cambiado de EN_BODEGA a EN_RUTA",
    "occurredAt": "2026-11-05T09:15:40.002Z"
  }
}
```

## 3. Tópicos y consumer groups

| Tópico             | Part. / Rép. | cleanup.policy   | Retención | Productor -> consumer groups |
|--------------------|--------------|------------------|-----------|------------------------------|
| `shipments.events` | 3 / 3        | `delete`         | 7 días    | shipments -> `ms-rutaexpress-audit`, `ms-rutaexpress-report`, `ms-rutaexpress-notify` |
| `audit.timeline`   | 3 / 3        | `compact,delete` | 30 días   | shipments -> `ms-rutaexpress-audit` |
| `notify.DLT`       | 3 / 3        | `delete`         | 14 días   | DeadLetterPublishingRecoverer de notify |
| `audit.DLT`        | 3 / 3        | `delete`         | 14 días   | DeadLetterPublishingRecoverer de audit |
| `report.DLT`       | 3 / 3        | `delete`         | 14 días   | DeadLetterPublishingRecoverer de report |

Todos los tópicos con `min.insync.replicas=2`. El productor usa `acks=all` e idempotencia, así un
mensaje solo se confirma cuando está en al menos dos brokers.

Cada consumidor tiene su propio consumer group (recibe todos los eventos) y concurrencia 3, una
hebra por partición.

## 4. Cómo usa notify el evento

notify escucha `shipments.events` y, ante un `ShipmentStatusChanged` cuyo `status` está en
`rutaexpress.kafka.notify.email-states` (por defecto `EN_RUTA`, `ENTREGADO`, `CANCELADO`), publica
el comando `EmailSendCommand` de siempre en `cmd.direct` con routing key `email.send`. El flujo de
RabbitMQ (colas, reintentos, DLQ) no cambia.

shipments sigue publicando sus propios comandos de email por RabbitMQ. Para que el cliente no reciba
dos correos, los dos caminos usan el mismo `eventId` para el comando de email de un envío y estado:
`email-<shipmentId>-<status>`. El `EmailCommandListener` de notify descarta el segundo por
deduplicación. Un envío no puede volver a un estado anterior, así que el id nunca se repite para
correos distintos.

## 5. Pendientes conocidos

- `serviceType`: shipments solo conoce el `serviceId`; catalog no expone una consulta interna del
  servicio por id. Mientras no exista, `serviceType` lleva el `serviceId` como texto. Cuando catalog
  agregue esa consulta, el campo pasará a llevar el nombre del servicio sin cambiar el contrato.
- `recipientEmail` y `recipientName` viajan en `shipments.events` (retención de 7 días). Si el
  equipo prefiere no tener datos personales en Kafka, notify tendría que consultarlos a shipments.
