package df.digitalfix_ms_admin_queues.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/** Resultado del aprovisionamiento idempotente de tópicos core en Apache Kafka. */
@Data
@Builder
public class KafkaProvisionResultDTO {

    private boolean created;
    private String message;
    private List<String> createdTopics;
    private List<String> existingTopics;
    private List<String> topics;
}
