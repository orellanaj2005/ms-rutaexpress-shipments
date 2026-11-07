package cl.rutaexpress.shipments.messaging;

public record ShipmentCreatedPayload(Long shipmentId, Long serviceId, String serviceType, String status,
                                     String origin, String destination, String createdBy) {
}
