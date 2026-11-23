package df.digitalfix_ms_admin_queues.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Respuesta consolidada de la inicialización idempotente unificada (RabbitMQ + Apache Kafka)
 * expuesta por {@code POST /api/v1/queues/initialize}.
 */
@Data
@Builder
public class InitializeResponseDTO {

    /** {@code SUCCESS}, {@code PARTIAL} o {@code FAILED}. */
    private String status;
    private String message;
    private RabbitInitResult rabbitmq;
    private KafkaInitResult kafka;

    @Data
    @Builder
    public static class RabbitInitResult {
        private String status;
        private boolean created;
        private String message;
        private List<String> exchanges;
        private List<String> queues;
        private List<String> bindings;
        private String error;
    }

    @Data
    @Builder
    public static class KafkaInitResult {
        private String status;
        private boolean created;
        private String message;
        private List<String> createdTopics;
        private List<String> existingTopics;
        private List<String> topics;
        private String error;
    }
}
