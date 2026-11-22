package df.digitalfix_ms_admin_queues.exception;

/**
 * El broker no aceptó el evento (no respondió, lo rechazó o ninguna cola lo recibió). Se responde
 * 503 para que el productor sepa que el evento no quedó encolado.
 */
public class PublicacionFallidaException extends AdminQueueException {

    public PublicacionFallidaException(String message) {
        super(message);
    }

    public PublicacionFallidaException(String message, Throwable cause) {
        super(message, cause);
    }
}
