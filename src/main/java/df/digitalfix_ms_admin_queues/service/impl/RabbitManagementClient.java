package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.exception.AdminQueueException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.Map;

/**
 * Cliente HTTP hacia la RabbitMQ Management API (puerto 15672) para consultar
 * el inventario real de colas, exchanges y bindings del virtual host configurado.
 */
@Slf4j
@Component
public class RabbitManagementClient {

    private static final ParameterizedTypeReference<List<Map<String, Object>>> LIST_TYPE =
            new ParameterizedTypeReference<>() {};

    private final RestClient managementClient;
    private final String virtualHost;

    public RabbitManagementClient(
            @Value("${app.admin.management-url}") String managementUrl,
            @Value("${spring.rabbitmq.username}") String username,
            @Value("${spring.rabbitmq.password}") String password,
            @Value("${spring.rabbitmq.virtual-host:/}") String virtualHost) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(10000);
        this.managementClient = RestClient.builder()
                .baseUrl(managementUrl)
                .defaultHeaders(headers -> headers.setBasicAuth(username, password))
                .requestFactory(factory)
                .build();
        this.virtualHost = virtualHost;
    }

    public List<Map<String, Object>> listQueues() {
        return fetchList("/api/queues/{vhost}", "colas");
    }

    public List<Map<String, Object>> listExchanges() {
        return fetchList("/api/exchanges/{vhost}", "exchanges");
    }

    public List<Map<String, Object>> listBindings() {
        return fetchList("/api/bindings/{vhost}", "bindings");
    }

    private List<Map<String, Object>> fetchList(String path, String recurso) {
        try {
            List<Map<String, Object>> body = managementClient.get()
                    .uri(path, virtualHost)
                    .retrieve()
                    .body(LIST_TYPE);
            return body != null ? body : List.of();
        } catch (RestClientException e) {
            log.error("Error al consultar {} en RabbitMQ Management API: {}", recurso, e.getMessage());
            throw new AdminQueueException("No se pudo consultar " + recurso + " desde RabbitMQ Management API", e);
        }
    }
}
