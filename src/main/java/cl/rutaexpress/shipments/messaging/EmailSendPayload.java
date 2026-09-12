package cl.rutaexpress.shipments.messaging;

public record EmailSendPayload(Long shipmentId, String recipientEmail, String recipientName,
                                String event, String message) {
}
