package cl.rutaexpress.shipments.config;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares only the {@code cmd.direct} exchange that shipments publishes commands
 * to. Per the shared team topology (see docs/guia_javier_rutaexpress.md section 7
 * "Topologia RabbitMQ"), the queues, DLQs, and the other two exchanges
 * (cmd.topic, cmd.dead.dlx) are owned and declared by notify-svc (the consumer).
 * Declaring the exchange here as well is idempotent and safe.
 */
@Configuration
public class RabbitConfig {

    @Value("${messaging.rabbitmq.exchange}")
    private String exchangeName;

    @Bean
    public DirectExchange cmdDirectExchange() {
        return new DirectExchange(exchangeName, true, false);
    }
}
