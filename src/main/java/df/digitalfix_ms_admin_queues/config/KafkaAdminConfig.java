package df.digitalfix_ms_admin_queues.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Configura {@link KafkaAdmin} y el {@link AdminClient} nativo de Apache Kafka para gestionar
 * tópicos, particiones y políticas de retención de forma centralizada desde {@code ms-admin-queues}.
 */
@Configuration
@EnableConfigurationProperties(KafkaTopologyProperties.class)
public class KafkaAdminConfig {

    @Bean
    public KafkaAdmin kafkaAdmin(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            KafkaTopologyProperties properties) {
        int timeoutMs = properties.getAdmin().getRequestTimeoutMs();

        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);

        KafkaAdmin kafkaAdmin = new KafkaAdmin(configs);
        // El aprovisionamiento se ejecuta de forma controlada vía /initialize (tolerante si Kafka no ha iniciado).
        kafkaAdmin.setAutoCreate(false);
        return kafkaAdmin;
    }

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
