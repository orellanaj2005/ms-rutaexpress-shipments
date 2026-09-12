package cl.rutaexpress.shipments.exception;

public class ShipmentNotFoundException extends RuntimeException {
    public ShipmentNotFoundException(Long id) {
        super("Shipment not found: " + id);
    }
}
