package cl.rutaexpress.shipments.service;

import cl.rutaexpress.shipments.client.CatalogClient;
import cl.rutaexpress.shipments.domain.Shipment;
import cl.rutaexpress.shipments.domain.ShipmentStatus;
import cl.rutaexpress.shipments.domain.ShipmentStatusTransitionValidator;
import cl.rutaexpress.shipments.dto.CreateShipmentRequest;
import cl.rutaexpress.shipments.exception.InvalidStatusTransitionException;
import cl.rutaexpress.shipments.exception.ShipmentNotFoundException;
import cl.rutaexpress.shipments.messaging.*;
import cl.rutaexpress.shipments.repository.ShipmentRepository;
import cl.rutaexpress.shipments.repository.ShipmentSpecifications;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

@Service
public class ShipmentService {

    private final ShipmentRepository shipmentRepository;
    private final CatalogClient catalogClient;
    private final ShipmentEventPublisher eventPublisher;
    private final CommandPublisher commandPublisher;

    public ShipmentService(ShipmentRepository shipmentRepository,
                            CatalogClient catalogClient,
                            ShipmentEventPublisher eventPublisher,
                            CommandPublisher commandPublisher) {
        this.shipmentRepository = shipmentRepository;
        this.catalogClient = catalogClient;
        this.eventPublisher = eventPublisher;
        this.commandPublisher = commandPublisher;
    }

    @Transactional
    public Shipment create(CreateShipmentRequest request) {
        Shipment shipment = new Shipment(
                request.originAddress(),
                request.destinationAddress(),
                request.recipientName(),
                request.recipientEmail(),
                request.recipientPhone(),
                request.serviceId(),
                request.weightKg(),
                request.declaredValue()
        );
        return shipmentRepository.save(shipment);
    }

    @Transactional(readOnly = true)
    public Shipment getById(Long id) {
        return shipmentRepository.findById(id)
                .orElseThrow(() -> new ShipmentNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public Page<Shipment> search(ShipmentStatus status, LocalDate from, LocalDate to, Pageable pageable) {
        Instant fromInstant = from == null ? null : from.atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant toInstant = to == null ? null : to.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();

        Specification<Shipment> spec = Specification.allOf(
                ShipmentSpecifications.hasStatus(status),
                ShipmentSpecifications.createdFrom(fromInstant),
                ShipmentSpecifications.createdBefore(toInstant)
        );
        return shipmentRepository.findAll(spec, pageable);
    }

    /**
     * Transitions a shipment to a new status, applying the catalog capacity check
     * (for ACEPTADO), persisting the change, and publishing events/commands.
     * Steps run strictly in order: load -> validate transition -> catalog call
     * (only if ACEPTADO) -> persist -> publish Kafka event -> publish RabbitMQ
     * commands. Catalog/persistence failures abort before any status mutation.
     */
    @Transactional
    public Shipment changeStatus(Long id, ShipmentStatus newStatus) {
        Shipment shipment = shipmentRepository.findById(id)
                .orElseThrow(() -> new ShipmentNotFoundException(id));

        ShipmentStatus previousStatus = shipment.getStatus();

        if (!ShipmentStatusTransitionValidator.isValidTransition(previousStatus, newStatus)) {
            throw new InvalidStatusTransitionException(
                    "Cannot transition shipment " + id + " from " + previousStatus + " to " + newStatus);
        }

        if (newStatus == ShipmentStatus.ACEPTADO) {
            catalogClient.decreaseCapacity(shipment.getServiceId());
        }

        shipment.setStatus(newStatus);
        // saveAndFlush forces the optimistic-lock check to happen now, before we
        // publish any events, instead of being deferred to transaction commit.
        Shipment saved = shipmentRepository.saveAndFlush(shipment);

        eventPublisher.publishStatusChanged(saved.getId(), previousStatus.name(), newStatus.name());
        publishCommands(saved, newStatus);

        return saved;
    }

    private void publishCommands(Shipment shipment, ShipmentStatus newStatus) {
        Long id = shipment.getId();
        switch (newStatus) {
            case ACEPTADO -> commandPublisher.publish("email.send", EventEnvelope.of(
                    "EmailSendCommand", id.toString(),
                    new EmailSendPayload(id, shipment.getRecipientEmail(), shipment.getRecipientName(),
                            "ACEPTADO", "Tu envío ha sido aceptado")));
            case EN_BODEGA -> commandPublisher.publish("warehouse.ticket", EventEnvelope.of(
                    "WarehouseTicketCommand", id.toString(),
                    new WarehouseTicketPayload(id, shipment.getOriginAddress(),
                            shipment.getDestinationAddress(), shipment.getWeightKg())));
            case EN_RUTA -> {
                commandPublisher.publish("email.send", EventEnvelope.of(
                        "EmailSendCommand", id.toString(),
                        new EmailSendPayload(id, shipment.getRecipientEmail(), shipment.getRecipientName(),
                                "EN_RUTA", "Tu envío está en ruta")));
                commandPublisher.publish("label.gen", EventEnvelope.of(
                        "LabelGenCommand", id.toString(),
                        new LabelGenPayload(id, shipment.getRecipientName(), shipment.getDestinationAddress(),
                                shipment.getWeightKg(), shipment.getServiceId())));
            }
            case ENTREGADO -> commandPublisher.publish("email.send", EventEnvelope.of(
                    "EmailSendCommand", id.toString(),
                    new EmailSendPayload(id, shipment.getRecipientEmail(), shipment.getRecipientName(),
                            "ENTREGADO", "Tu envío ha sido entregado")));
            case CANCELADO -> {
                // No RabbitMQ command for CANCELADO; Kafka event already published above.
            }
            default -> {
                // CREADO is never a transition target; no command to publish.
            }
        }
    }
}
