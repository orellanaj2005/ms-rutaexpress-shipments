package cl.rutaexpress.shipments.dto;

import java.util.Map;

public record ErrorResponse(String error, String message, Map<String, String> fields) {
    public ErrorResponse(String error, String message) {
        this(error, message, null);
    }
}
