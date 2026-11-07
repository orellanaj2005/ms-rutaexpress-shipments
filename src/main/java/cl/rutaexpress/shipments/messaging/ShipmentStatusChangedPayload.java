package cl.rutaexpress.shipments.messaging;

import java.time.Instant;

public record ShipmentStatusChangedPayload(Long shipmentId, String previousStatus, String status,
                                           String serviceType, String changedBy, Instant changedAt,
                                           String recipientEmail, String recipientName) {
}
