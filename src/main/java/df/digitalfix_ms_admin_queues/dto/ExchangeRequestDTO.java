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
public class ExchangeRequestDTO {

    @NotBlank(message = "El nombre del exchange (exchangeName) es obligatorio")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre del exchange solo puede contener caracteres alfanuméricos, puntos, guiones y guiones bajos")
    private String exchangeName;

    @NotBlank(message = "El tipo de exchange es obligatorio (ej. direct, topic, fanout, headers)")
    @Pattern(regexp = "^(?i)(direct|topic|fanout|headers)$", message = "El tipo de exchange debe ser uno de los siguientes: direct, topic, fanout o headers")
    private String type;

    @Builder.Default
    private boolean durable = true;

    @Builder.Default
    private boolean autoDelete = false;
}
