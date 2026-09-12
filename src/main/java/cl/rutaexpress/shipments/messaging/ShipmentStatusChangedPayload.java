package cl.rutaexpress.shipments.messaging;

import java.time.Instant;

public record ShipmentStatusChangedPayload(Long shipmentId, String previousStatus, String newStatus,
                                            Instant occurredAt) {
}
