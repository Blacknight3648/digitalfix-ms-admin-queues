package df.digitalfix_ms_admin_queues.service;

import df.digitalfix_ms_admin_queues.dto.KafkaProvisionResultDTO;
import df.digitalfix_ms_admin_queues.dto.TopicDetailDTO;
import df.digitalfix_ms_admin_queues.dto.TopicRequestDTO;
import df.digitalfix_ms_admin_queues.dto.TopicSummaryDTO;

import java.util.List;

/**
 * Servicio de administración de tópicos en Apache Kafka mediante {@code AdminClient}.
 */
public interface KafkaAdminService {

    List<TopicSummaryDTO> listTopics();

    TopicDetailDTO getTopicDetails(String name);

    TopicSummaryDTO createTopic(TopicRequestDTO request);

    void deleteTopic(String name);

    KafkaProvisionResultDTO provisionCoreTopics();
}
