package cl.rutaexpress.shipments.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;

public record CreateShipmentRequest(
        @NotBlank String originAddress,
        @NotBlank String destinationAddress,
        @NotBlank String recipientName,
        @NotBlank @Email String recipientEmail,
        String recipientPhone,
        @NotNull Long serviceId,
        @NotNull @Positive BigDecimal weightKg,
        @PositiveOrZero BigDecimal declaredValue
) {
}
