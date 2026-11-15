package cl.rutaexpress.shipments.messaging;

import cl.rutaexpress.shipments.events.ShipmentCreatedEvent;
import cl.rutaexpress.shipments.events.ShipmentStatusChangedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Publica en {@code shipments.events} los hechos de negocio del envío, con el shipmentId como clave. */
@Component
public class ShipmentEventPublisher {

    public static final String TYPE_CREATED = "ShipmentCreated";
    public static final String TYPE_STATUS_CHANGED = "ShipmentStatusChanged";

    private final KafkaEventSender sender;
    private final String topic;

    public ShipmentEventPublisher(KafkaEventSender sender,
                                  @Value("${messaging.kafka.topic-events}") String topic) {
        this.sender = sender;
        this.topic = topic;
    }

    public void publishCreated(ShipmentCreatedEvent event) {
        sender.send(topic, EventEnvelope.of(TYPE_CREATED, event.shipmentId(), event.occurredAt(), event.traceId(),
                new ShipmentCreatedPayload(event.shipmentId(), event.serviceId(), serviceType(event.serviceId()),
                        event.status(), event.origin(), event.destination(), event.actor().name())));
    }

    public void publishStatusChanged(ShipmentStatusChangedEvent event) {
        sender.send(topic, EventEnvelope.of(TYPE_STATUS_CHANGED, event.shipmentId(), event.occurredAt(),
                event.traceId(),
                new ShipmentStatusChangedPayload(event.shipmentId(), event.previousStatus(), event.status(),
                        serviceType(event.serviceId()), event.actor().name(), event.occurredAt(),
                        event.recipientEmail(), event.recipientName())));
    }

    /** Pendiente en docs/eventos.md: catalog aún no expone el nombre del servicio por id. */
    private static String serviceType(Long serviceId) {
        return serviceId == null ? null : serviceId.toString();
    }
}
