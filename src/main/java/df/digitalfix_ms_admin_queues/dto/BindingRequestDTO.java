package df.digitalfix_ms_admin_queues.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static df.digitalfix_ms_admin_queues.dto.ReglasNombresAmqp.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BindingRequestDTO {

    @NotBlank(message = "El nombre de la cola origen (queueName) es obligatorio")
    @Pattern(regexp = NOMBRE, message = "El nombre de la cola " + NOMBRE_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "El nombre de la cola no puede superar 255 caracteres")
    private String queueName;

    @NotBlank(message = "El nombre del exchange destino (exchangeName) es obligatorio")
    @Pattern(regexp = NOMBRE, message = "El nombre del exchange " + NOMBRE_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "El nombre del exchange no puede superar 255 caracteres")
    private String exchangeName;

    @NotBlank(message = "La clave de enrutamiento (routingKey) es obligatoria")
    @Pattern(regexp = ROUTING_KEY, message = "La routing key " + ROUTING_KEY_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "La routing key no puede superar 255 caracteres")
    private String routingKey;
}
