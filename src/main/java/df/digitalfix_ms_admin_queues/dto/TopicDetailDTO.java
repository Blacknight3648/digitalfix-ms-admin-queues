package df.digitalfix_ms_admin_queues.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

/** Detalle completo de un tópico en Apache Kafka: particiones, réplicas y configuraciones. */
@Data
@Builder
public class TopicDetailDTO {

    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean internal;
    private List<PartitionInfoDTO> partitionDetails;
    private Map<String, String> configs;

    @Data
    @Builder
    public static class PartitionInfoDTO {
        private int partition;
        private int leader;
        private List<Integer> replicas;
        private List<Integer> isr;
    }
}
