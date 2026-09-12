package cl.rutaexpress.shipments.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Publishes command messages to the {@code cmd.direct} exchange (see RabbitConfig
 * for topology notes). Best-effort: failures are logged but never propagated.
 */
@Component
public class CommandPublisher {

    private static final Logger log = LoggerFactory.getLogger(CommandPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final String exchange;

    public CommandPublisher(RabbitTemplate rabbitTemplate,
                             ObjectMapper objectMapper,
                             @Value("${messaging.rabbitmq.exchange}") String exchange) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.exchange = exchange;
    }

    public void publish(String routingKey, EventEnvelope<?> envelope) {
        try {
            String json = objectMapper.writeValueAsString(envelope);
            rabbitTemplate.convertAndSend(exchange, routingKey, json);
        } catch (Exception e) {
            log.error("Failed to publish command to routingKey={} on exchange={}", routingKey, exchange, e);
        }
    }
}
