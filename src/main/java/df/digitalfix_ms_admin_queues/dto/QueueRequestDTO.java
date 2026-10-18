package df.digitalfix_ms_admin_queues.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueRequestDTO {

    @NotBlank(message = "El nombre de la cola (queueName) es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre de la cola solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String queueName;

    @Builder.Default
    private boolean durable = true;

    @Builder.Default
    private boolean exclusive = false;

    @Builder.Default
    private boolean autoDelete = false;

    // Argumentos opcionales para configuración de Dead Letter Exchange (DLX)
    private String deadLetterExchange;
    private String deadLetterRoutingKey;
    private Long messageTtl; // TTL opcional en milisegundos
}
