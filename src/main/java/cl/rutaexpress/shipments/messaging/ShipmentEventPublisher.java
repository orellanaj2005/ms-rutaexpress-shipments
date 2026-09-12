package cl.rutaexpress.shipments.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Publishes ShipmentStatusChanged events to Kafka. Best-effort: any failure is
 * logged but never propagated, since messaging must not fail the HTTP response.
 */
@Component
public class ShipmentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ShipmentEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public ShipmentEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                   ObjectMapper objectMapper,
                                   @Value("${messaging.kafka.topic-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publishStatusChanged(Long shipmentId, String previousStatus, String newStatus) {
        try {
            EventEnvelope<ShipmentStatusChangedPayload> envelope = EventEnvelope.of(
                    "ShipmentStatusChanged",
                    shipmentId.toString(),
                    new ShipmentStatusChangedPayload(shipmentId, previousStatus, newStatus,
                            java.time.Instant.now()));
            String json = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(topic, shipmentId.toString(), json);
        } catch (Exception e) {
            log.error("Failed to publish ShipmentStatusChanged event to Kafka for shipmentId={}", shipmentId, e);
        }
    }
}
