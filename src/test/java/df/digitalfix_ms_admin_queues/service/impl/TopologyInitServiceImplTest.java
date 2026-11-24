package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.dto.InitializeResponseDTO;
import df.digitalfix_ms_admin_queues.dto.KafkaProvisionResultDTO;
import df.digitalfix_ms_admin_queues.dto.QueueInitializeResponseDTO;
import df.digitalfix_ms_admin_queues.exception.BrokerUnavailableException;
import df.digitalfix_ms_admin_queues.service.KafkaAdminService;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopologyInitServiceImplTest {

    private RabbitAdminService rabbitAdminService;
    private KafkaAdminService kafkaAdminService;
    private TopologyInitServiceImpl service;

    @BeforeEach
    void setUp() {
        rabbitAdminService = mock(RabbitAdminService.class);
        kafkaAdminService = mock(KafkaAdminService.class);
        service = new TopologyInitServiceImpl(rabbitAdminService, kafkaAdminService);
    }

    @Test
    void retornaSuccessCuandoAmbosBrokersRespondenBien() {
        when(rabbitAdminService.initializeTopology()).thenReturn(QueueInitializeResponseDTO.builder()
                .created(true).message("OK").exchanges(List.of("cmd.direct")).queues(List.of("q.cmd.email")).bindings(List.of()).build());
        when(kafkaAdminService.provisionCoreTopics()).thenReturn(KafkaProvisionResultDTO.builder()
                .created(true).message("OK").createdTopics(List.of("workorders.events")).existingTopics(List.of()).topics(List.of("workorders.events")).build());

        InitializeResponseDTO result = service.initializeAll();

        assertThat(result.getStatus()).isEqualTo("SUCCESS");
        assertThat(result.isCreated()).isTrue();
        assertThat(result.getRabbitmq().getStatus()).isEqualTo("OK");
        assertThat(result.getKafka().getStatus()).isEqualTo("OK");
    }

    @Test
    void retornaPartialCuandoUnBrokerFalla() {
        when(rabbitAdminService.initializeTopology()).thenReturn(QueueInitializeResponseDTO.builder()
                .created(false).message("Ya existía").exchanges(List.of()).queues(List.of()).bindings(List.of()).build());
        when(kafkaAdminService.provisionCoreTopics())
                .thenThrow(new BrokerUnavailableException("Kafka caído", new RuntimeException("timeout")));

        InitializeResponseDTO result = service.initializeAll();

        assertThat(result.getStatus()).isEqualTo("PARTIAL");
        assertThat(result.getRabbitmq().getStatus()).isEqualTo("OK");
        assertThat(result.getKafka().getStatus()).isEqualTo("FAILED");
        assertThat(result.getKafka().getError()).contains("Kafka caído");
    }

    @Test
    void retornaFailedCuandoAmbosBrokersFallan() {
        when(rabbitAdminService.initializeTopology()).thenThrow(new RuntimeException("Rabbit caído"));
        when(kafkaAdminService.provisionCoreTopics()).thenThrow(new RuntimeException("Kafka caído"));

        InitializeResponseDTO result = service.initializeAll();

        assertThat(result.getStatus()).isEqualTo("FAILED");
        assertThat(result.getRabbitmq().getStatus()).isEqualTo("FAILED");
        assertThat(result.getKafka().getStatus()).isEqualTo("FAILED");
    }
}
