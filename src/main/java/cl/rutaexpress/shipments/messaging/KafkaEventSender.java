package cl.rutaexpress.shipments.messaging;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Envío best-effort a Kafka. El resultado se registra de forma asíncrona: INFO con tópico,
 * partición y offset si el broker confirma; ERROR con el eventId si falla. Ninguna falla se propaga,
 * porque la mensajería no debe romper la respuesta HTTP de una operación ya confirmada en la base.
 */
@Component
public class KafkaEventSender {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventSender.class);

    private final KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate;

    public KafkaEventSender(KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, EventEnvelope<?> envelope) {
        try {
            kafkaTemplate.send(topic, envelope.aggregateId(), envelope).whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("Evento publicado type={} eventId={} key={} topic={} partition={} offset={}",
                            envelope.type(), envelope.eventId(), envelope.aggregateId(),
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    logFailure(topic, envelope, ex);
                }
            });
        } catch (Exception ex) {
            // send() también puede fallar antes de devolver el future, por ejemplo si no obtiene
            // la metadata del tópico dentro de max.block.ms.
            logFailure(topic, envelope, ex);
        }
    }

    private static void logFailure(String topic, EventEnvelope<?> envelope, Throwable ex) {
        log.error("No se pudo publicar el evento type={} eventId={} key={} topic={}",
                envelope.type(), envelope.eventId(), envelope.aggregateId(), topic, ex);
    }
}
