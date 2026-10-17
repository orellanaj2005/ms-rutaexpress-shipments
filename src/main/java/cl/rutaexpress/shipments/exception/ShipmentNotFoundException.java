package cl.rutaexpress.shipments.exception;

public class ShipmentNotFoundException extends RuntimeException {
    public ShipmentNotFoundException(Long id) {
        super("No existe el envío " + id + ".");
    }
}
