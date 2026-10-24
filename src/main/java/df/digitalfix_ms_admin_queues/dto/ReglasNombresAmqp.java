package df.digitalfix_ms_admin_queues.dto;

/**
 * Formato de los nombres que acepta la API de administración, compartido por los DTO y los
 * parámetros de ruta. Se limita a lo que usa la topología del caso (q.cmd.email, cmd.dead.dlx,
 * dispatch.#): letras, números, puntos, guiones y guiones bajos; las routing keys además admiten
 * los comodines '*' y '#' de los exchanges topic.
 */
public final class ReglasNombresAmqp {

    public static final String NOMBRE = "^[a-zA-Z0-9._-]+$";
    public static final String NOMBRE_MENSAJE = "solo puede contener letras, números, puntos, guiones y guiones bajos";

    public static final String ROUTING_KEY = "^[a-zA-Z0-9._*#-]+$";
    public static final String ROUTING_KEY_MENSAJE = "solo puede contener letras, números, puntos, guiones, '*' y '#'";

    public static final int LARGO_MAXIMO = 255;

    private ReglasNombresAmqp() {
    }
}
