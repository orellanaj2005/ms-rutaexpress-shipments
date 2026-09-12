package cl.rutaexpress.shipments.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Enforces the allowed shipment status transition graph. This is a pure,
 * Spring-free class so it can be unit tested in isolation.
 */
public final class ShipmentStatusTransitionValidator {

    private static final Map<ShipmentStatus, Set<ShipmentStatus>> ALLOWED_TRANSITIONS = buildTransitionMap();

    private ShipmentStatusTransitionValidator() {
    }

    private static Map<ShipmentStatus, Set<ShipmentStatus>> buildTransitionMap() {
        Map<ShipmentStatus, Set<ShipmentStatus>> map = new EnumMap<>(ShipmentStatus.class);
        map.put(ShipmentStatus.CREADO, EnumSet.of(ShipmentStatus.ACEPTADO, ShipmentStatus.CANCELADO));
        map.put(ShipmentStatus.ACEPTADO, EnumSet.of(ShipmentStatus.EN_BODEGA, ShipmentStatus.CANCELADO));
        map.put(ShipmentStatus.EN_BODEGA, EnumSet.of(ShipmentStatus.EN_RUTA, ShipmentStatus.CANCELADO));
        map.put(ShipmentStatus.EN_RUTA, EnumSet.of(ShipmentStatus.ENTREGADO));
        map.put(ShipmentStatus.ENTREGADO, EnumSet.noneOf(ShipmentStatus.class));
        map.put(ShipmentStatus.CANCELADO, EnumSet.noneOf(ShipmentStatus.class));
        return map;
    }

    /**
     * @return true if transitioning from {@code current} to {@code target} is allowed.
     */
    public static boolean isValidTransition(ShipmentStatus current, ShipmentStatus target) {
        if (current == null || target == null) {
            return false;
        }
        return ALLOWED_TRANSITIONS.getOrDefault(current, Set.of()).contains(target);
    }
}
