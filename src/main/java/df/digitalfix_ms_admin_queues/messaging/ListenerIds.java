package df.digitalfix_ms_admin_queues.messaging;

/**
 * Identificadores de los contenedores de listeners. Con ellos la API de administración consulta,
 * detiene o reanuda cada consumidor (GET/POST /api/v1/listeners/{id}). Se mantienen los mismos ids
 * que usaba ms-notify (listener-email, listener-dispatch, listener-report, listener-dlq), así el
 * panel de Mensajería del frontend no cambia.
 */
public final class ListenerIds {

    public static final String DLQ = "listener-dlq";

    private static final String PREFIJO = "listener-";

    private ListenerIds() {
    }

    /** Id del listener de un flujo: {@code email} → {@code listener-email}. */
    public static String deFlujo(String nombreFlujo) {
        return PREFIJO + nombreFlujo;
    }
}
