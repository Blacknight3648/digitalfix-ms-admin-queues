package df.digitalfix_ms_admin_queues.service;

import df.digitalfix_ms_admin_queues.dto.InitializeResponseDTO;

/**
 * Orquesta la inicialización idempotente unificada de la topología en RabbitMQ y Apache Kafka.
 */
public interface TopologyInitService {

    InitializeResponseDTO initializeAll();
}
