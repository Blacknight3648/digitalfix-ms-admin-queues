package df.digitalfix_ms_admin_queues.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

/**
 * Evento que un microservicio productor (ms-workorders) pide publicar. El productor solo dice
 * <em>qué</em> pasó y en qué flujo; ms-admin-queues decide exchange y routing key, arma el sobre
 * común ({@code eventId}, {@code timestamp}...) y lo publica.
 *
 * @param eventType     tipo de negocio del evento (ej. WORK_ORDER_STATUS_CHANGED)
 * @param correlationId agrupa los mensajes de una misma entidad (ej. OT-15)
 * @param traceId       opcional; si no viene se genera uno
 * @param subtipo       opcional; si viene se publica por el exchange topic con
 *                      {@code <prefijo>.<subtipo>} (ej. email.asignada); si no, por el direct
 * @param data          contenido de negocio; ms-admin-queues no lo interpreta, solo lo transporta
 */
public record PublicarEventoRequestDTO(
        @NotBlank(message = "El tipo de evento (eventType) es obligatorio")
        @Pattern(regexp = "^[A-Za-z0-9._-]+$", message = "El eventType solo puede contener letras, números, puntos, guiones y guiones bajos")
        @Size(max = 100, message = "El eventType no puede superar 100 caracteres")
        String eventType,

        @Size(max = 100, message = "El correlationId no puede superar 100 caracteres")
        String correlationId,

        @Size(max = 100, message = "El traceId no puede superar 100 caracteres")
        String traceId,

        // Una sola palabra: el patrón email.* calza con exactamente una palabra después del prefijo.
        @Pattern(regexp = "^$|^[A-Za-z0-9_-]+$", message = "El subtipo es una sola palabra: letras, números, guiones y guiones bajos")
        @Size(max = 60, message = "El subtipo no puede superar 60 caracteres")
        String subtipo,

        @NotNull(message = "El contenido del evento (data) es obligatorio")
        JsonNode data
) {

    @JsonIgnore
    @AssertTrue(message = "El contenido del evento (data) debe ser un objeto JSON")
    public boolean isDataObjeto() {
        return data == null || data.isObject();
    }

    @JsonIgnore
    public boolean tieneSubtipo() {
        return subtipo != null && !subtipo.isBlank();
    }
}
