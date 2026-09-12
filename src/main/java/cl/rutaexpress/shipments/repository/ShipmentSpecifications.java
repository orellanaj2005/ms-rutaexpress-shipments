package cl.rutaexpress.shipments.repository;

import cl.rutaexpress.shipments.domain.Shipment;
import cl.rutaexpress.shipments.domain.ShipmentStatus;
import org.springframework.data.jpa.domain.Specification;

import java.time.Instant;

public final class ShipmentSpecifications {

    private ShipmentSpecifications() {
    }

    public static Specification<Shipment> hasStatus(ShipmentStatus status) {
        return (root, query, cb) -> status == null ? null : cb.equal(root.get("status"), status);
    }

    public static Specification<Shipment> createdFrom(Instant from) {
        return (root, query, cb) -> from == null ? null : cb.greaterThanOrEqualTo(root.get("createdAt"), from);
    }

    public static Specification<Shipment> createdBefore(Instant to) {
        return (root, query, cb) -> to == null ? null : cb.lessThan(root.get("createdAt"), to);
    }
}
