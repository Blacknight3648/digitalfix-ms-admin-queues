package df.digitalfix_ms_admin_queues.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
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
public class ExchangeRequestDTO {

    @NotBlank(message = "El nombre del exchange (exchangeName) es obligatorio")
    @Pattern(regexp = NOMBRE, message = "El nombre del exchange " + NOMBRE_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "El nombre del exchange no puede superar 255 caracteres")
    private String exchangeName;

    @NotBlank(message = "El tipo de exchange es obligatorio (ej. direct, topic, fanout, headers)")
    @Pattern(regexp = "^(?i)(direct|topic|fanout|headers)$", message = "El tipo de exchange debe ser uno de los siguientes: direct, topic, fanout o headers")
    private String type;

    @Builder.Default
    private boolean durable = true;

    @Builder.Default
    private boolean autoDelete = false;
}
