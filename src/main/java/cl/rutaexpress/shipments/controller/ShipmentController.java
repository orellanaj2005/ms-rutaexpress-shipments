package cl.rutaexpress.shipments.controller;

import cl.rutaexpress.shipments.domain.Shipment;
import cl.rutaexpress.shipments.domain.ShipmentStatus;
import cl.rutaexpress.shipments.dto.CreateShipmentRequest;
import cl.rutaexpress.shipments.dto.ShipmentResponse;
import cl.rutaexpress.shipments.dto.UpdateStatusRequest;
import cl.rutaexpress.shipments.exception.InvalidStatusTransitionException;
import cl.rutaexpress.shipments.service.ShipmentService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.time.LocalDate;

@RestController
@RequestMapping("/api/shipments")
public class ShipmentController {

    private final ShipmentService shipmentService;

    public ShipmentController(ShipmentService shipmentService) {
        this.shipmentService = shipmentService;
    }

    @PostMapping
    public ResponseEntity<ShipmentResponse> create(@Valid @RequestBody CreateShipmentRequest request) {
        Shipment created = shipmentService.create(request);
        return ResponseEntity.created(URI.create("/api/shipments/" + created.getId()))
                .body(ShipmentResponse.from(created));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ShipmentResponse> getById(@PathVariable Long id) {
        Shipment shipment = shipmentService.getById(id);
        return ResponseEntity.ok(ShipmentResponse.from(shipment));
    }

    @PutMapping("/{id}/status")
    public ResponseEntity<ShipmentResponse> updateStatus(@PathVariable Long id,
                                                           @Valid @RequestBody UpdateStatusRequest request) {
        ShipmentStatus newStatus = parseStatus(request.status());
        Shipment updated = shipmentService.changeStatus(id, newStatus);
        return ResponseEntity.ok(ShipmentResponse.from(updated));
    }

    @GetMapping
    public ResponseEntity<Page<ShipmentResponse>> search(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) LocalDate from,
            @RequestParam(required = false) LocalDate to,
            Pageable pageable) {
        ShipmentStatus statusFilter = status == null ? null : parseStatus(status);
        Page<ShipmentResponse> page = shipmentService.search(statusFilter, from, to, pageable)
                .map(ShipmentResponse::from);
        return ResponseEntity.ok(page);
    }

    private ShipmentStatus parseStatus(String raw) {
        try {
            return ShipmentStatus.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidStatusTransitionException("Unknown status: " + raw);
        }
    }
}
