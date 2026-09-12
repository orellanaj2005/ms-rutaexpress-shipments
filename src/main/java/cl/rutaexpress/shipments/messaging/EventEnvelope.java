package cl.rutaexpress.shipments.messaging;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(String type, String eventId, Instant timestamp, String traceId,
                                String correlationId, T payload) {

    public static <T> EventEnvelope<T> of(String type, String correlationId, T payload) {
        return new EventEnvelope<>(type, UUID.randomUUID().toString(), Instant.now(),
                UUID.randomUUID().toString(), correlationId, payload);
    }
}
