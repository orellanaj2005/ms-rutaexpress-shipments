package cl.rutaexpress.shipments.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Sobre común de los eventos Kafka (ver docs/eventos.md). El {@code aggregateId} es el id del envío
 * y se usa también como clave del mensaje, para que todos los eventos de un envío caigan en la
 * misma partición y se lean en orden.
 */
public record EventEnvelope<T>(String eventId, String type, int version, String aggregateId,
                               Instant timestamp, String traceId, String correlationId,
                               String producer, T payload) {

    public static final String PRODUCER = "ms-rutaexpress-shipments";
    public static final int VERSION = 1;

    public static <T> EventEnvelope<T> of(String type, Long shipmentId, Instant timestamp,
                                          String traceId, T payload) {
        String id = shipmentId.toString();
        return new EventEnvelope<>(UUID.randomUUID().toString(), type, VERSION, id, timestamp,
                traceId, id, PRODUCER, payload);
    }
}
