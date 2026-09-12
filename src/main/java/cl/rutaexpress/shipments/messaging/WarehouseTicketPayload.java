package cl.rutaexpress.shipments.messaging;

import java.math.BigDecimal;

public record WarehouseTicketPayload(Long shipmentId, String originAddress, String destinationAddress,
                                      BigDecimal weightKg) {
}
