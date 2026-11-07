package cl.rutaexpress.shipments.messaging;

import cl.rutaexpress.shipments.events.ShipmentCreatedEvent;
import cl.rutaexpress.shipments.events.ShipmentStatusChangedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Publica en {@code audit.timeline} cada acción de un usuario sobre un envío, con actor y rol del JWT. */
@Component
public class AuditTimelinePublisher {

    public static final String TYPE = "AuditAction";
    public static final String ACTION_CREATE = "CREAR_ENVIO";
    public static final String ACTION_CHANGE_STATUS = "CAMBIAR_ESTADO";
    public static final String ACTION_CANCEL = "CANCELAR_ENVIO";

    private final KafkaEventSender sender;
    private final String topic;

    public AuditTimelinePublisher(KafkaEventSender sender,
                                  @Value("${messaging.kafka.topic-audit}") String topic) {
        this.sender = sender;
        this.topic = topic;
    }

    public void publishCreated(ShipmentCreatedEvent event) {
        publish(event.shipmentId(), event.traceId(), new AuditActionPayload(event.shipmentId(),
                event.actor().name(), event.actor().role(), ACTION_CREATE,
                "Envío creado de " + event.origin() + " a " + event.destination(), event.occurredAt()));
    }

    public void publishStatusChanged(ShipmentStatusChangedEvent event) {
        boolean cancel = "CANCELADO".equals(event.status());
        String details = cancel
                ? "Envío cancelado (estaba " + event.previousStatus() + ")"
                : "Estado cambiado de " + event.previousStatus() + " a " + event.status();
        publish(event.shipmentId(), event.traceId(), new AuditActionPayload(event.shipmentId(),
                event.actor().name(), event.actor().role(), cancel ? ACTION_CANCEL : ACTION_CHANGE_STATUS,
                details, event.occurredAt()));
    }

    private void publish(Long shipmentId, String traceId, AuditActionPayload payload) {
        sender.send(topic, EventEnvelope.of(TYPE, shipmentId, payload.occurredAt(), traceId, payload));
    }
}
