package df.digitalfix_ms_admin_queues.service;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.dto.ExchangeRequestDTO;
import df.digitalfix_ms_admin_queues.dto.QueueInitializeResponseDTO;
import df.digitalfix_ms_admin_queues.dto.QueueRequestDTO;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface RabbitAdminService {

    // Inicialización idempotente de topología base
    QueueInitializeResponseDTO initializeTopology();

    // Gestión de Colas (Queues)
    List<Map<String, Object>> listQueues();
    void createQueue(QueueRequestDTO queueRequestDTO);
    boolean deleteQueue(String queueName, boolean unusedOnly, boolean emptyOnly);
    int purgeQueue(String queueName);
    Properties getQueueProperties(String queueName);

    // Gestión de Exchanges
    List<Map<String, Object>> listExchanges();
    void createExchange(ExchangeRequestDTO exchangeRequestDTO);
    boolean deleteExchange(String exchangeName);

    // Gestión de Bindings (Enlaces)
    List<Map<String, Object>> listBindings();
    void declareBinding(BindingRequestDTO bindingRequestDTO);
    void removeBinding(BindingRequestDTO bindingRequestDTO);
}
