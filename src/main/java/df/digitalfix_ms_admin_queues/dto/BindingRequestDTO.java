package df.digitalfix_ms_admin_queues.dto;

import jakarta.validation.constraints.NotBlank;
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
    private String queueName;

    @NotBlank(message = "El nombre del exchange destino (exchangeName) es obligatorio")
    private String exchangeName;

    @NotBlank(message = "La clave de enrutamiento (routingKey) es obligatoria")
    private String routingKey;
}
