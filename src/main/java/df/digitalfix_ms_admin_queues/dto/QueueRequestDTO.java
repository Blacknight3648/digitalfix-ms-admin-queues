package df.digitalfix_ms_admin_queues.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.Pattern;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
// Constructor completo privado (solo lo usa el @Builder): así Jackson 3 deserializa con el
// constructor vacío y respeta los valores por defecto (durable=true) de los campos omitidos.
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class QueueRequestDTO {

    @NotBlank(message = "El nombre de la cola (queueName) es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre de la cola solo puede contener letras, números, puntos, guiones y guiones bajos")
    @Size(max = 255, message = "El nombre de la cola no puede superar 255 caracteres")
    private String queueName;

    @Builder.Default
    private boolean durable = true;

    @Builder.Default
    private boolean exclusive = false;

    @Builder.Default
    private boolean autoDelete = false;

    // Argumentos opcionales para configuración de Dead Letter Exchange (DLX)
    @Pattern(regexp = "^[a-zA-Z0-9._-]*$", message = "El nombre del dead letter exchange solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String deadLetterExchange;

    @Pattern(regexp = "^[a-zA-Z0-9._*#-]*$", message = "La routing key de dead letter solo puede contener letras, números, puntos, guiones, '*' y '#'")
    private String deadLetterRoutingKey;

    @Positive(message = "El TTL (messageTtl) debe ser un número positivo de milisegundos")
    private Long messageTtl; // TTL opcional en milisegundos
}
