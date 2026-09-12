package cl.rutaexpress.shipments.messaging;

import java.math.BigDecimal;

public record LabelGenPayload(Long shipmentId, String recipientName, String destinationAddress,
                               BigDecimal weightKg, Long serviceId) {
}
