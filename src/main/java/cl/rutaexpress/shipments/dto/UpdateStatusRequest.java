package cl.rutaexpress.shipments.dto;

import jakarta.validation.constraints.NotNull;

public record UpdateStatusRequest(
        @NotNull String status
) {
}
