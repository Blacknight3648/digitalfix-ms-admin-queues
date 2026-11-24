package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.dto.InitializeResponseDTO;
import df.digitalfix_ms_admin_queues.dto.KafkaProvisionResultDTO;
import df.digitalfix_ms_admin_queues.dto.QueueInitializeResponseDTO;
import df.digitalfix_ms_admin_queues.service.KafkaAdminService;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import df.digitalfix_ms_admin_queues.service.TopologyInitService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopologyInitServiceImpl implements TopologyInitService {

    private final RabbitAdminService rabbitAdminService;
    private final KafkaAdminService kafkaAdminService;

    @Override
    public InitializeResponseDTO initializeAll() {
        InitializeResponseDTO.RabbitInitResult rabbitResult = initializeRabbit();
        InitializeResponseDTO.KafkaInitResult kafkaResult = initializeKafka();

        boolean rabbitOk = "OK".equals(rabbitResult.getStatus());
        boolean kafkaOk = "OK".equals(kafkaResult.getStatus());

        String status;
        String message;
        if (rabbitOk && kafkaOk) {
            status = "SUCCESS";
            message = "Topología unificada de RabbitMQ y Kafka asegurada exitosamente.";
        } else if (rabbitOk || kafkaOk) {
            status = "PARTIAL";
            message = "Inicialización parcial: uno de los brokers presentó fallos.";
        } else {
            status = "FAILED";
            message = "No se pudo inicializar ni RabbitMQ ni Apache Kafka.";
        }

        return InitializeResponseDTO.builder()
                .status(status)
                .created(rabbitResult.isCreated() || kafkaResult.isCreated())
                .message(message)
                .rabbitmq(rabbitResult)
                .kafka(kafkaResult)
                .build();
    }

    private InitializeResponseDTO.RabbitInitResult initializeRabbit() {
        try {
            QueueInitializeResponseDTO result = rabbitAdminService.initializeTopology();
            return InitializeResponseDTO.RabbitInitResult.builder()
                    .status("OK")
                    .created(result.isCreated())
                    .message(result.getMessage())
                    .exchanges(result.getExchanges())
                    .queues(result.getQueues())
                    .bindings(result.getBindings())
                    .build();
        } catch (Exception e) {
            log.error("Fallo al inicializar RabbitMQ durante /initialize: {}", e.getMessage());
            return InitializeResponseDTO.RabbitInitResult.builder()
                    .status("FAILED")
                    .created(false)
                    .exchanges(List.of())
                    .queues(List.of())
                    .bindings(List.of())
                    .error(e.getMessage())
                    .build();
        }
    }

    private InitializeResponseDTO.KafkaInitResult initializeKafka() {
        try {
            KafkaProvisionResultDTO result = kafkaAdminService.provisionCoreTopics();
            return InitializeResponseDTO.KafkaInitResult.builder()
                    .status("OK")
                    .created(result.isCreated())
                    .message(result.getMessage())
                    .createdTopics(result.getCreatedTopics())
                    .existingTopics(result.getExistingTopics())
                    .topics(result.getTopics())
                    .build();
        } catch (Exception e) {
            log.error("Fallo al inicializar Apache Kafka durante /initialize: {}", e.getMessage());
            return InitializeResponseDTO.KafkaInitResult.builder()
                    .status("FAILED")
                    .created(false)
                    .createdTopics(List.of())
                    .existingTopics(List.of())
                    .topics(List.of())
                    .error(e.getMessage())
                    .build();
        }
    }
}
