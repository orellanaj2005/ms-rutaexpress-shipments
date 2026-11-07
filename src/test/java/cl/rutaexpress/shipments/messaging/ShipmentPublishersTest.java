package cl.rutaexpress.shipments.messaging;

import cl.rutaexpress.shipments.events.ShipmentCreatedEvent;
import cl.rutaexpress.shipments.events.ShipmentStatusChangedEvent;
import cl.rutaexpress.shipments.security.CurrentUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShipmentPublishersTest {

    private static final CurrentUser OPERADOR = new CurrentUser("operador@rutaexpress.cl", "Operador");
    private static final Instant NOW = Instant.parse("2026-11-05T09:15:40Z");

    private KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate;
    private ShipmentEventPublisher shipmentEvents;
    private AuditTimelinePublisher auditTimeline;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        KafkaEventSender sender = new KafkaEventSender(kafkaTemplate);
        shipmentEvents = new ShipmentEventPublisher(sender, "shipments.events");
        auditTimeline = new AuditTimelinePublisher(sender, "audit.timeline");
    }

    @Test
    void shipmentCreatedUsesShipmentIdAsKeyAndContractFields() {
        shipmentEvents.publishCreated(created());

        EventEnvelope<?> envelope = captureSent("shipments.events", "42");
        assertThat(envelope.type()).isEqualTo("ShipmentCreated");
        assertThat(envelope.version()).isEqualTo(1);
        assertThat(envelope.aggregateId()).isEqualTo("42");
        assertThat(envelope.producer()).isEqualTo("ms-rutaexpress-shipments");
        assertThat(envelope.traceId()).isEqualTo("trace-1");
        assertThat(envelope.eventId()).isNotBlank();
        assertThat(envelope.payload()).isEqualTo(new ShipmentCreatedPayload(42L, 3L, "3", "CREADO",
                "Origen 1", "Destino 2", "operador@rutaexpress.cl"));
    }

    @Test
    void statusChangedSendsStatusNotNewStatus() {
        shipmentEvents.publishStatusChanged(statusChanged("EN_RUTA"));

        EventEnvelope<?> envelope = captureSent("shipments.events", "42");
        assertThat(envelope.type()).isEqualTo("ShipmentStatusChanged");
        ShipmentStatusChangedPayload payload = (ShipmentStatusChangedPayload) envelope.payload();
        assertThat(payload.previousStatus()).isEqualTo("EN_BODEGA");
        assertThat(payload.status()).isEqualTo("EN_RUTA");
        assertThat(payload.changedBy()).isEqualTo("operador@rutaexpress.cl");
        assertThat(payload.changedAt()).isEqualTo(NOW);
        assertThat(payload.recipientEmail()).isEqualTo("ana@correo.cl");
    }

    @Test
    void auditTimelineCarriesActorAndRole() {
        auditTimeline.publishCreated(created());

        AuditActionPayload payload = (AuditActionPayload) captureSent("audit.timeline", "42").payload();
        assertThat(payload.actor()).isEqualTo("operador@rutaexpress.cl");
        assertThat(payload.role()).isEqualTo("Operador");
        assertThat(payload.action()).isEqualTo("CREAR_ENVIO");
    }

    @Test
    void cancellationIsRecordedAsCancelAction() {
        auditTimeline.publishStatusChanged(statusChanged("CANCELADO"));

        AuditActionPayload payload = (AuditActionPayload) captureSent("audit.timeline", "42").payload();
        assertThat(payload.action()).isEqualTo("CANCELAR_ENVIO");
    }

    @Test
    void failedSendIsLoggedAndNeverPropagated() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker caído")));
        assertThatCode(() -> shipmentEvents.publishCreated(created())).doesNotThrowAnyException();

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new IllegalStateException("sin metadata"));
        assertThatCode(() -> auditTimeline.publishCreated(created())).doesNotThrowAnyException();
    }

    private EventEnvelope<?> captureSent(String topic, String key) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<EventEnvelope<?>> captor = (ArgumentCaptor) ArgumentCaptor.forClass(EventEnvelope.class);
        verify(kafkaTemplate).send(eq(topic), eq(key), captor.capture());
        return captor.getValue();
    }

    private static ShipmentCreatedEvent created() {
        return new ShipmentCreatedEvent(42L, 3L, "CREADO", "Origen 1", "Destino 2", OPERADOR, "trace-1", NOW);
    }

    private static ShipmentStatusChangedEvent statusChanged(String status) {
        return new ShipmentStatusChangedEvent(42L, 3L, "EN_BODEGA", status, "ana@correo.cl", "Ana",
                OPERADOR, "trace-2", NOW);
    }
}
