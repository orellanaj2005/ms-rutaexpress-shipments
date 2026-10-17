package cl.rutaexpress.shipments.messaging;

import cl.rutaexpress.shipments.domain.Shipment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Publica en {@code cmd.direct} los comandos que shipments pide a notify. ShipmentService solo
 * llama a estos métodos de negocio; exchange y routing keys salen de application.yaml.
 *
 * <p>Con publisher confirms y returns activos (ver application.yaml), todo mensaje que el broker
 * rechace (NACK) o no pueda enrutar a ninguna cola queda en un log ERROR con su eventId.
 * La publicación es best-effort: una falla se registra y no revierte el cambio de estado.
 */
@Component
public class ShipmentCommandPublisher {

    private static final Logger log = LoggerFactory.getLogger(ShipmentCommandPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final String exchange;
    private final String emailRoutingKey;
    private final String warehouseRoutingKey;
    private final String labelRoutingKey;

    public ShipmentCommandPublisher(RabbitTemplate rabbitTemplate,
                                    ObjectMapper objectMapper,
                                    @Value("${rutaexpress.rabbitmq.exchanges.direct}") String exchange,
                                    @Value("${rutaexpress.rabbitmq.email.routing-key}") String emailRoutingKey,
                                    @Value("${rutaexpress.rabbitmq.warehouse.routing-key}") String warehouseRoutingKey,
                                    @Value("${rutaexpress.rabbitmq.label.routing-key}") String labelRoutingKey) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.exchange = exchange;
        this.emailRoutingKey = emailRoutingKey;
        this.warehouseRoutingKey = warehouseRoutingKey;
        this.labelRoutingKey = labelRoutingKey;

        rabbitTemplate.setConfirmCallback((correlation, ack, cause) -> {
            if (!ack) {
                log.error("El broker rechazó el comando eventId={} motivo={}",
                        correlation == null ? "desconocido" : correlation.getId(), cause);
            }
        });
        rabbitTemplate.setReturnsCallback(returned -> log.error(
                "Comando sin cola de destino: exchange={} routingKey={} replyCode={} motivo={}",
                returned.getExchange(), returned.getRoutingKey(), returned.getReplyCode(), returned.getReplyText()));
    }

    /** Correo al destinatario con el nuevo estado del envío. */
    public void notificarCliente(Shipment shipment, String event, String message) {
        publish(emailRoutingKey, EventEnvelope.of("EmailSendCommand", shipment.getId().toString(),
                new EmailSendPayload(shipment.getId(), shipment.getRecipientEmail(), shipment.getRecipientName(),
                        event, message)));
    }

    /** Ticket de picking para la bodega. */
    public void solicitarTicketBodega(Shipment shipment) {
        publish(warehouseRoutingKey, EventEnvelope.of("WarehouseTicketCommand", shipment.getId().toString(),
                new WarehouseTicketPayload(shipment.getId(), shipment.getOriginAddress(),
                        shipment.getDestinationAddress(), shipment.getWeightKg())));
    }

    /** Etiqueta de despacho en PDF. */
    public void solicitarEtiqueta(Shipment shipment) {
        publish(labelRoutingKey, EventEnvelope.of("LabelGenCommand", shipment.getId().toString(),
                new LabelGenPayload(shipment.getId(), shipment.getRecipientName(), shipment.getDestinationAddress(),
                        shipment.getWeightKg(), shipment.getServiceId())));
    }

    private void publish(String routingKey, EventEnvelope<?> envelope) {
        try {
            String json = objectMapper.writeValueAsString(envelope);
            rabbitTemplate.convertAndSend(exchange, routingKey, json, new CorrelationData(envelope.eventId()));
        } catch (Exception e) {
            log.error("No se pudo publicar el comando eventId={} routingKey={} exchange={}",
                    envelope.eventId(), routingKey, exchange, e);
        }
    }
}
