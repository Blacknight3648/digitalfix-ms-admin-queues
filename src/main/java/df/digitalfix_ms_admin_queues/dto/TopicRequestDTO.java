package df.digitalfix_ms_admin_queues.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static df.digitalfix_ms_admin_queues.dto.ReglasNombresKafka.LARGO_MAXIMO;
import static df.digitalfix_ms_admin_queues.dto.ReglasNombresKafka.NOMBRE;
import static df.digitalfix_ms_admin_queues.dto.ReglasNombresKafka.NOMBRE_MENSAJE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TopicRequestDTO {

    @NotBlank(message = "El nombre del tópico (name) es obligatorio")
    @Pattern(regexp = NOMBRE, message = "El nombre del tópico " + NOMBRE_MENSAJE)
    @Size(max = LARGO_MAXIMO, message = "El nombre del tópico no puede superar 249 caracteres")
    private String name;

    @NotNull(message = "El número de particiones (partitions) es obligatorio")
    @Min(value = 3, message = "El número mínimo de particiones es 3")
    @Builder.Default
    private Integer partitions = 3;

    /** Factor de réplica opcional: si se omite, se aplica {@code kafka.default-replicas}. */
    @Min(value = 1, message = "El factor de réplica (replicationFactor) debe ser al menos 1")
    private Integer replicationFactor;

    /** Configuraciones opcionales del tópico (ej. cleanup.policy, retention.ms). */
    private Map<String, String> configs;

    @JsonIgnore
    @AssertTrue(message = "El nombre del tópico no puede ser '.', '..' ni usar el prefijo reservado '__'")
    public boolean isNombrePermitido() {
        if (name == null) {
            return true;
        }
        return !name.equals(".") && !name.equals("..") && !name.startsWith("__");
    }
}
