package cl.rutaexpress.shipments.config;

import cl.rutaexpress.shipments.messaging.EventEnvelope;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Productor Kafka tipado: clave String (shipmentId) y valor {@link EventEnvelope} serializado a JSON.
 * Las propiedades de confiabilidad (acks, idempotencia, reintentos, timeouts, compresión) se leen de
 * {@code spring.kafka.producer} en application.yaml, para que queden visibles en un solo lugar.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, EventEnvelope<?>> producerFactory(KafkaProperties kafkaProperties,
                                                                     JsonMapper jsonMapper) {
        JacksonJsonSerializer<EventEnvelope<?>> valueSerializer = new JacksonJsonSerializer<>(jsonMapper);
        // Sin cabecera __TypeId__: los consumidores no dependen de las clases Java del productor.
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(),
                new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, EventEnvelope<?>> kafkaTemplate(ProducerFactory<String, EventEnvelope<?>> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
