package cl.rutaexpress.shipments.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure unit test, no Spring context: verifies the strict transition graph.
 */
class ShipmentStatusTransitionValidatorTest {

    @ParameterizedTest
    @CsvSource({
            "CREADO, ACEPTADO",
            "CREADO, CANCELADO",
            "ACEPTADO, EN_BODEGA",
            "ACEPTADO, CANCELADO",
            "EN_BODEGA, EN_RUTA",
            "EN_BODEGA, CANCELADO",
            "EN_RUTA, ENTREGADO"
    })
    void validTransitionsAreAllowed(ShipmentStatus current, ShipmentStatus target) {
        assertThat(ShipmentStatusTransitionValidator.isValidTransition(current, target)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
            "CREADO, EN_RUTA",
            "CREADO, EN_BODEGA",
            "CREADO, ENTREGADO",
            "ACEPTADO, EN_RUTA",
            "ACEPTADO, ENTREGADO",
            "EN_BODEGA, ACEPTADO",
            "EN_BODEGA, ENTREGADO",
            "EN_RUTA, CANCELADO",
            "EN_RUTA, CREADO",
            "EN_RUTA, ACEPTADO"
    })
    void invalidTransitionsAreRejected(ShipmentStatus current, ShipmentStatus target) {
        assertThat(ShipmentStatusTransitionValidator.isValidTransition(current, target)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ShipmentStatus.class)
    void entregadoIsTerminal(ShipmentStatus target) {
        assertThat(ShipmentStatusTransitionValidator.isValidTransition(ShipmentStatus.ENTREGADO, target)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ShipmentStatus.class)
    void canceladoIsTerminal(ShipmentStatus target) {
        assertThat(ShipmentStatusTransitionValidator.isValidTransition(ShipmentStatus.CANCELADO, target)).isFalse();
    }

    @Test
    void nullCurrentOrTargetIsRejected() {
        assertThat(ShipmentStatusTransitionValidator.isValidTransition(null, ShipmentStatus.ACEPTADO)).isFalse();
        assertThat(ShipmentStatusTransitionValidator.isValidTransition(ShipmentStatus.CREADO, null)).isFalse();
    }
}
