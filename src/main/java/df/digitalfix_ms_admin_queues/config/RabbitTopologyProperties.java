package df.digitalfix_ms_admin_queues.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Propiedades centralizadas de la topología de RabbitMQ definidas en application.yaml (app.topology).
 */
@Data
@ConfigurationProperties(prefix = "app.topology")
public class RabbitTopologyProperties {

    private List<ExchangeDefinition> exchanges = new ArrayList<>();
    private List<QueueDefinition> queues = new ArrayList<>();

    @Data
    public static class ExchangeDefinition {
        private String name;
        private String type = "DIRECT";
        private boolean durable = true;
        private boolean autoDelete = false;
    }

    @Data
    public static class QueueDefinition {
        private String name;
        private boolean durable = true;
        private String deadLetterExchange;
        private String deadLetterRoutingKey;
        private String dlqName;
        private Long dlqMessageTtl;
        private List<BindingDefinition> bindings = new ArrayList<>();
    }

    @Data
    public static class BindingDefinition {
        private String exchange;
        private String routingKey;
    }
}
