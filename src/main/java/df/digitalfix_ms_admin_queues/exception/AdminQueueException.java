package df.digitalfix_ms_admin_queues.exception;

public class AdminQueueException extends RuntimeException {

    public AdminQueueException(String message) {
        super(message);
    }

    public AdminQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
