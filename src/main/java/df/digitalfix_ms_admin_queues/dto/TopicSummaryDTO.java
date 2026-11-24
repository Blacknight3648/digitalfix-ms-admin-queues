package df.digitalfix_ms_admin_queues.dto;

import lombok.Builder;
import lombok.Data;

/** Resumen de un tópico de negocio en Apache Kafka para el listado general. */
@Data
@Builder
public class TopicSummaryDTO {

    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean internal;
}
