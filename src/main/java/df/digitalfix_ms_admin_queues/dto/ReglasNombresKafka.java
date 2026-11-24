package df.digitalfix_ms_admin_queues.dto;

/**
 * Reglas de nomenclatura para tópicos de Apache Kafka compartidas por los DTO y parámetros de ruta.
 * Kafka permite letras, números, puntos, guiones y guiones bajos hasta 249 caracteres (excluyendo
 * {@code .}, {@code ..} y el prefijo interno {@code __}).
 */
public final class ReglasNombresKafka {

    public static final String NOMBRE = "^[a-zA-Z0-9._-]+$";
    public static final String NOMBRE_MENSAJE = "solo puede contener letras, números, puntos, guiones y guiones bajos";
    public static final int LARGO_MAXIMO = 249;

    private ReglasNombresKafka() {
    }
}
