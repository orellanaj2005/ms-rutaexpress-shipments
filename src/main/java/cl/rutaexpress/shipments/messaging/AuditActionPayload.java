package cl.rutaexpress.shipments.messaging;

import java.time.Instant;

public record AuditActionPayload(Long shipmentId, String actor, String role, String action,
                                 String details, Instant occurredAt) {
}
