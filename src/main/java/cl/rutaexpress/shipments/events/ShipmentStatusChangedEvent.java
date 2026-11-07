package cl.rutaexpress.shipments.events;

import cl.rutaexpress.shipments.security.CurrentUser;

import java.time.Instant;

/** Evento de aplicación: sale a Kafka solo después del commit (ver {@link ShipmentKafkaEventsListener}). */
public record ShipmentStatusChangedEvent(Long shipmentId, Long serviceId, String previousStatus, String status,
                                         String recipientEmail, String recipientName, CurrentUser actor,
                                         String traceId, Instant occurredAt) {
}
