package df.digitalfix_ms_admin_queues.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Respuesta de la inicialización idempotente de la topología base en RabbitMQ.
 */
@Data
@Builder
public class QueueInitializeResponseDTO {

    private boolean created;
    private String message;
    private List<String> exchanges;
    private List<String> queues;
    private List<String> bindings;
}
