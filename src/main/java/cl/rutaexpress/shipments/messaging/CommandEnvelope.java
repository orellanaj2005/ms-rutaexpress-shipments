package cl.rutaexpress.shipments.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Sobre de los comandos RabbitMQ hacia notify. Conserva la forma que notify ya lee, separada del
 * {@link EventEnvelope} de Kafka para que un cambio en un contrato no rompa al otro.
 */
public record CommandEnvelope<T>(String type, String eventId, Instant timestamp, String traceId,
                                 String correlationId, T payload) {

    public static <T> CommandEnvelope<T> of(String type, String correlationId, T payload) {
        return withEventId(type, UUID.randomUUID().toString(), correlationId, payload);
    }

    public static <T> CommandEnvelope<T> withEventId(String type, String eventId, String correlationId, T payload) {
        return new CommandEnvelope<>(type, eventId, Instant.now(), UUID.randomUUID().toString(),
                correlationId, payload);
    }

    /**
     * Id del comando de email de un envío en un estado. notify arma el mismo id cuando genera el
     * correo desde Kafka, y su deduplicación evita que el cliente reciba dos correos iguales.
     */
    public static String emailEventId(Long shipmentId, String status) {
        return "email-" + shipmentId + "-" + status;
    }
}
