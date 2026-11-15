package cl.rutaexpress.shipments.events;

import cl.rutaexpress.shipments.messaging.AuditTimelinePublisher;
import cl.rutaexpress.shipments.messaging.ShipmentEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Publica en Kafka los eventos de aplicación de ShipmentService una vez confirmada la transacción.
 * Si la transacción se revierte, Spring descarta el evento y no sale nada a Kafka.
 */
@Component
public class ShipmentKafkaEventsListener {

    private final ShipmentEventPublisher shipmentEvents;
    private final AuditTimelinePublisher auditTimeline;

    public ShipmentKafkaEventsListener(ShipmentEventPublisher shipmentEvents, AuditTimelinePublisher auditTimeline) {
        this.shipmentEvents = shipmentEvents;
        this.auditTimeline = auditTimeline;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onCreated(ShipmentCreatedEvent event) {
        shipmentEvents.publishCreated(event);
        auditTimeline.publishCreated(event);
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onStatusChanged(ShipmentStatusChangedEvent event) {
        shipmentEvents.publishStatusChanged(event);
        auditTimeline.publishStatusChanged(event);
    }
}
