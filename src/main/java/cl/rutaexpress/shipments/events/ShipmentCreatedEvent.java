package cl.rutaexpress.shipments.events;

import cl.rutaexpress.shipments.security.CurrentUser;

import java.time.Instant;

/** Evento de aplicación: sale a Kafka solo después del commit (ver {@link ShipmentKafkaEventsListener}). */
public record ShipmentCreatedEvent(Long shipmentId, Long serviceId, String status, String origin,
                                   String destination, CurrentUser actor, String traceId, Instant occurredAt) {
}
