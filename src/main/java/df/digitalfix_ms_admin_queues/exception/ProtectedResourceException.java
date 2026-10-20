package df.digitalfix_ms_admin_queues.exception;

/** Se intentó borrar o desvincular un recurso del sistema (app.admin.protected-resources). */
public class ProtectedResourceException extends AdminQueueException {

    public ProtectedResourceException(String message) {
        super(message);
    }
}
