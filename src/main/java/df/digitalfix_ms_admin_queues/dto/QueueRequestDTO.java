package df.digitalfix_ms_admin_queues.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static df.digitalfix_ms_admin_queues.dto.ReglasNombresAmqp.*;

@Data
@Builder
@NoArgsConstructor
// Constructor completo privado (solo lo usa el @Builder): así Jackson 3 deserializa con el
// constructor vacío y respeta los valores por defecto (durable=true) de los campos omitidos.
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class QueueRequestDTO {

    /** TTL máximo aceptado: 7 días (la DLQ del caso guarda 24 h). */
    static final long TTL_MAXIMO_MS = 7L * 24 * 60 * 60 * 1000;

    @NotBlank(message = "El nombre de la cola (queueName) es obligatorio")
    @Pattern(regexp = NOMBRE, message = "El nombre de la cola " + NOMBRE_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "El nombre de la cola no puede superar 255 caracteres")
    private String queueName;

    @Builder.Default
    private boolean durable = true;

    // Una cola exclusiva pertenece a la conexión que la declara (la de ms-admin-queues): ningún
    // microservicio podría consumirla y RabbitMQ la borraría al reiniciar este servicio.
    @Builder.Default
    @AssertFalse(message = "exclusive=true no se permite: la cola quedaría ligada a la conexión de ms-admin-queues y nadie más podría consumirla")
    private boolean exclusive = false;

    @Builder.Default
    private boolean autoDelete = false;

    // Argumentos opcionales para configuración de Dead Letter Exchange (DLX)
    @Pattern(regexp = "^$|" + NOMBRE, message = "El nombre del dead letter exchange " + NOMBRE_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "El nombre del dead letter exchange no puede superar 255 caracteres")
    private String deadLetterExchange;

    @Pattern(regexp = "^$|" + ROUTING_KEY, message = "La routing key de dead letter " + ROUTING_KEY_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "La routing key de dead letter no puede superar 255 caracteres")
    private String deadLetterRoutingKey;

    @Positive(message = "El TTL (messageTtl) debe ser un número positivo de milisegundos")
    @Max(value = TTL_MAXIMO_MS, message = "El TTL (messageTtl) no puede superar 7 días (604800000 ms)")
    private Long messageTtl; // TTL opcional en milisegundos

    /** Una routing key de DLQ sin DLX no tendría efecto: RabbitMQ la ignoraría en silencio. */
    @JsonIgnore
    @AssertTrue(message = "deadLetterRoutingKey requiere deadLetterExchange: sin DLX los mensajes rechazados se descartan")
    public boolean isDeadLetterConsistente() {
        boolean conRoutingKey = deadLetterRoutingKey != null && !deadLetterRoutingKey.isBlank();
        boolean conExchange = deadLetterExchange != null && !deadLetterExchange.isBlank();
        return !conRoutingKey || conExchange;
    }
}
