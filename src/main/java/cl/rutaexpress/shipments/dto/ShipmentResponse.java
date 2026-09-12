package cl.rutaexpress.shipments.dto;

import cl.rutaexpress.shipments.domain.Shipment;

import java.math.BigDecimal;
import java.time.Instant;

public record ShipmentResponse(
        Long id,
        String originAddress,
        String destinationAddress,
        String recipientName,
        String recipientEmail,
        String recipientPhone,
        Long serviceId,
        BigDecimal weightKg,
        BigDecimal declaredValue,
        String status,
        Instant createdAt,
        Instant updatedAt,
        int version
) {
    public static ShipmentResponse from(Shipment shipment) {
        return new ShipmentResponse(
                shipment.getId(),
                shipment.getOriginAddress(),
                shipment.getDestinationAddress(),
                shipment.getRecipientName(),
                shipment.getRecipientEmail(),
                shipment.getRecipientPhone(),
                shipment.getServiceId(),
                shipment.getWeightKg(),
                shipment.getDeclaredValue(),
                shipment.getStatus().name(),
                shipment.getCreatedAt(),
                shipment.getUpdatedAt(),
                shipment.getVersion()
        );
    }
}
