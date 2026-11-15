package cl.rutaexpress.shipments.service;

import cl.rutaexpress.shipments.client.CatalogClient;
import cl.rutaexpress.shipments.domain.Shipment;
import cl.rutaexpress.shipments.domain.ShipmentStatus;
import cl.rutaexpress.shipments.domain.ShipmentStatusTransitionValidator;
import cl.rutaexpress.shipments.dto.CreateShipmentRequest;
import cl.rutaexpress.shipments.exception.InvalidStatusTransitionException;
import cl.rutaexpress.shipments.exception.ShipmentNotFoundException;
import cl.rutaexpress.shipments.events.ShipmentCreatedEvent;
import cl.rutaexpress.shipments.events.ShipmentStatusChangedEvent;
import cl.rutaexpress.shipments.messaging.ShipmentCommandPublisher;
import cl.rutaexpress.shipments.repository.ShipmentRepository;
import cl.rutaexpress.shipments.repository.ShipmentSpecifications;
import cl.rutaexpress.shipments.security.CurrentUser;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class ShipmentService {

    private final ShipmentRepository shipmentRepository;
    private final CatalogClient catalogClient;
    private final ApplicationEventPublisher eventPublisher;
    private final ShipmentCommandPublisher commandPublisher;

    public ShipmentService(ShipmentRepository shipmentRepository,
                            CatalogClient catalogClient,
                            ApplicationEventPublisher eventPublisher,
                            ShipmentCommandPublisher commandPublisher) {
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
        Shipment saved = shipmentRepository.save(shipment);

        // Se publica en Kafka recién después del commit (ShipmentKafkaEventsListener).
        eventPublisher.publishEvent(new ShipmentCreatedEvent(saved.getId(), saved.getServiceId(),
                saved.getStatus().name(), saved.getOriginAddress(), saved.getDestinationAddress(),
                CurrentUser.fromSecurityContext(), UUID.randomUUID().toString(), saved.getCreatedAt()));
        return saved;
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
     * (only if ACEPTADO) -> persist -> publish RabbitMQ commands. The Kafka events
     * (shipments.events and audit.timeline) are emitted after commit, so a
     * rolled-back change never reaches Kafka. Catalog/persistence failures abort
     * before any status mutation.
     */
    @Transactional
    public Shipment changeStatus(Long id, ShipmentStatus newStatus) {
        Shipment shipment = shipmentRepository.findById(id)
                .orElseThrow(() -> new ShipmentNotFoundException(id));

        ShipmentStatus previousStatus = shipment.getStatus();

        if (!ShipmentStatusTransitionValidator.isValidTransition(previousStatus, newStatus)) {
            Set<ShipmentStatus> allowed = ShipmentStatusTransitionValidator.allowedFrom(previousStatus);
            throw new InvalidStatusTransitionException(allowed.isEmpty()
                    ? "El envío " + id + " está " + previousStatus + ", que es un estado final: ya no admite cambios."
                    : "No se puede cambiar el envío " + id + " de " + previousStatus + " a " + newStatus
                            + ". Desde " + previousStatus + " solo se permite pasar a: "
                            + allowed.stream().map(Enum::name).collect(Collectors.joining(", ")) + ".");
        }

        if (newStatus == ShipmentStatus.ACEPTADO) {
            catalogClient.decreaseCapacity(shipment.getServiceId());
        }

        shipment.setStatus(newStatus);
        // saveAndFlush forces the optimistic-lock check to happen now, before we
        // publish any events, instead of being deferred to transaction commit.
        Shipment saved = shipmentRepository.saveAndFlush(shipment);

        eventPublisher.publishEvent(new ShipmentStatusChangedEvent(saved.getId(), saved.getServiceId(),
                previousStatus.name(), newStatus.name(), saved.getRecipientEmail(), saved.getRecipientName(),
                CurrentUser.fromSecurityContext(), UUID.randomUUID().toString(), saved.getUpdatedAt()));
        publishCommands(saved, newStatus);

        return saved;
    }

    private void publishCommands(Shipment shipment, ShipmentStatus newStatus) {
        switch (newStatus) {
            case ACEPTADO -> commandPublisher.notificarCliente(shipment, "ACEPTADO", "Tu envío ha sido aceptado");
            case EN_BODEGA -> commandPublisher.solicitarTicketBodega(shipment);
            case EN_RUTA -> {
                commandPublisher.notificarCliente(shipment, "EN_RUTA", "Tu envío está en ruta");
                commandPublisher.solicitarEtiqueta(shipment);
            }
            case ENTREGADO -> commandPublisher.notificarCliente(shipment, "ENTREGADO", "Tu envío ha sido entregado");
            case CANCELADO -> {
                // No RabbitMQ command for CANCELADO; the Kafka event goes out after commit.
            }
            default -> {
                // CREADO is never a transition target; no command to publish.
            }
        }
    }
}
