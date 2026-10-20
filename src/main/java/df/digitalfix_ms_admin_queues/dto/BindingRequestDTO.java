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
public class BindingRequestDTO {

    @NotBlank(message = "El nombre de la cola origen (queueName) es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre de la cola solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String queueName;

    @NotBlank(message = "El nombre del exchange destino (exchangeName) es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre del exchange solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String exchangeName;

    @NotBlank(message = "La clave de enrutamiento (routingKey) es obligatoria")
    @Pattern(regexp = "^[a-zA-Z0-9._*#-]+$", message = "La routing key solo puede contener letras, números, puntos, guiones, '*' y '#'")
    private String routingKey;
}
