package df.digitalfix_ms_admin_queues.exception;

/** Se intentó operar contra un broker de mensajería que no responde o no está disponible. */
public class BrokerUnavailableException extends AdminQueueException {

    public BrokerUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
