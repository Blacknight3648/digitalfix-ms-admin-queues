package df.digitalfix_ms_admin_queues.service;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.dto.ExchangeRequestDTO;
import df.digitalfix_ms_admin_queues.dto.QueueRequestDTO;

import java.util.Properties;

public interface RabbitAdminService {

    // Gestión de Colas (Queues)
    void createQueue(QueueRequestDTO queueRequestDTO);
    boolean deleteQueue(String queueName, boolean unusedOnly, boolean emptyOnly);
    int purgeQueue(String queueName);
    Properties getQueueProperties(String queueName);

    // Gestión de Exchanges
    void createExchange(ExchangeRequestDTO exchangeRequestDTO);
    boolean deleteExchange(String exchangeName);

    // Gestión de Bindings (Enlaces)
    void declareBinding(BindingRequestDTO bindingRequestDTO);
    void removeBinding(BindingRequestDTO bindingRequestDTO);
}
