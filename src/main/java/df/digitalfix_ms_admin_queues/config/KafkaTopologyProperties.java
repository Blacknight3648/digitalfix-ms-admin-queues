package df.digitalfix_ms_admin_queues.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Propiedades centralizadas de tópicos y administración de Apache Kafka definidas en application.yaml (kafka.*).
 */
@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaTopologyProperties {

    private int defaultReplicas = 1;
    private AdminProperties admin = new AdminProperties();
    private List<TopicDefinition> topics = new ArrayList<>();

    @Data
    public static class AdminProperties {
        private int requestTimeoutMs = 5000;
    }

    @Data
    public static class TopicDefinition {
        private String name;
        private int partitions = 3;
        private Integer replicas;
        private Map<String, String> configs = new HashMap<>();
    }
}
