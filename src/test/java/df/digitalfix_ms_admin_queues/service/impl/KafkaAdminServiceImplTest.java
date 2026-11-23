package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.config.KafkaTopologyProperties;
import df.digitalfix_ms_admin_queues.dto.KafkaProvisionResultDTO;
import df.digitalfix_ms_admin_queues.dto.TopicDetailDTO;
import df.digitalfix_ms_admin_queues.dto.TopicRequestDTO;
import df.digitalfix_ms_admin_queues.dto.TopicSummaryDTO;
import df.digitalfix_ms_admin_queues.exception.BrokerUnavailableException;
import df.digitalfix_ms_admin_queues.exception.ProtectedResourceException;
import df.digitalfix_ms_admin_queues.exception.ResourceAlreadyExistsException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAdminServiceImplTest {

    private AdminClient adminClient;
    private KafkaTopologyProperties properties;
    private KafkaAdminServiceImpl service;

    @BeforeEach
    void setUp() {
        adminClient = mock(AdminClient.class);
        properties = buildProperties();
        service = new KafkaAdminServiceImpl(adminClient, properties);
        ReflectionTestUtils.setField(service, "protectedResources",
                List.of("q.cmd.*", "cmd.*", "workorders.events*", "audit.timeline"));
    }

    private KafkaTopologyProperties buildProperties() {
        KafkaTopologyProperties props = new KafkaTopologyProperties();
        props.setDefaultReplicas(1);

        KafkaTopologyProperties.TopicDefinition t1 = new KafkaTopologyProperties.TopicDefinition();
        t1.setName("workorders.events");
        t1.setPartitions(3);
        t1.setReplicas(1);
        t1.setConfigs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"));

        KafkaTopologyProperties.TopicDefinition t2 = new KafkaTopologyProperties.TopicDefinition();
        t2.setName("audit.timeline");
        t2.setPartitions(3);
        t2.setReplicas(1);
        t2.setConfigs(Map.of("cleanup.policy", "compact,delete", "retention.ms", "2592000000"));

        props.setTopics(List.of(t1, t2));
        return props;
    }

    private static <T> KafkaFuture<T> failedFuture(Throwable error) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
    }

    private static TopicDescription sampleDescription(String name, int partitions) {
        Node node = new Node(1, "kafka", 9092);
        List<TopicPartitionInfo> partitionInfos = java.util.stream.IntStream.range(0, partitions)
                .mapToObj(i -> new TopicPartitionInfo(i, node, List.of(node), List.of(node)))
                .toList();
        return new TopicDescription(name, false, partitionInfos);
    }

    @Test
    void listTopicsExcluyeTopicosInternos() {
        ListTopicsResult listResult = mock(ListTopicsResult.class);
        when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("__consumer_offsets", "workorders.events")));
        when(adminClient.listTopics(any(ListTopicsOptions.class))).thenReturn(listResult);

        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(describeResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of("workorders.events", sampleDescription("workorders.events", 3))));
        when(adminClient.describeTopics(List.of("workorders.events"))).thenReturn(describeResult);

        List<TopicSummaryDTO> result = service.listTopics();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("workorders.events");
        assertThat(result.get(0).getPartitions()).isEqualTo(3);
        assertThat(result.get(0).getReplicationFactor()).isEqualTo(1);
    }

    @Test
    void getTopicDetailsRetornaParticionesYConfigs() {
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(describeResult.topicNameValues()).thenReturn(
                Map.of("audit.timeline", KafkaFuture.completedFuture(sampleDescription("audit.timeline", 3))));
        when(adminClient.describeTopics(List.of("audit.timeline"))).thenReturn(describeResult);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "audit.timeline");
        DescribeConfigsResult configsResult = mock(DescribeConfigsResult.class);
        Config config = new Config(List.of(
                new ConfigEntry("cleanup.policy", "compact,delete"),
                new ConfigEntry("retention.ms", "2592000000")
        ));
        when(configsResult.values()).thenReturn(Map.of(resource, KafkaFuture.completedFuture(config)));
        when(adminClient.describeConfigs(List.of(resource))).thenReturn(configsResult);

        TopicDetailDTO detail = service.getTopicDetails("audit.timeline");

        assertThat(detail.getName()).isEqualTo("audit.timeline");
        assertThat(detail.getPartitionDetails()).hasSize(3);
        assertThat(detail.getConfigs())
                .containsEntry("cleanup.policy", "compact,delete")
                .containsEntry("retention.ms", "2592000000");
    }

    @Test
    void createTopicUsaReplicasPorDefectoYMapeaDuplicadoYTimeout() {
        CreateTopicsResult createOk = mock(CreateTopicsResult.class);
        when(createOk.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.createTopics(anyCollection())).thenReturn(createOk);

        TopicSummaryDTO created = service.createTopic(TopicRequestDTO.builder()
                .name("custom.events")
                .partitions(3)
                .build());

        assertThat(created.getReplicationFactor()).isEqualTo(1);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(adminClient).createTopics(captor.capture());
        NewTopic sent = captor.getValue().iterator().next();
        assertThat(sent.numPartitions()).isEqualTo(3);
        assertThat(sent.replicationFactor()).isEqualTo((short) 1);

        CreateTopicsResult createExists = mock(CreateTopicsResult.class);
        when(createExists.all()).thenReturn(failedFuture(new TopicExistsException("exists")));
        when(adminClient.createTopics(anyCollection())).thenReturn(createExists);

        assertThatThrownBy(() -> service.createTopic(TopicRequestDTO.builder().name("custom.events").partitions(3).build()))
                .isInstanceOf(ResourceAlreadyExistsException.class);

        CreateTopicsResult createTimeout = mock(CreateTopicsResult.class);
        when(createTimeout.all()).thenReturn(failedFuture(new TimeoutException("timeout")));
        when(adminClient.createTopics(anyCollection())).thenReturn(createTimeout);

        assertThatThrownBy(() -> service.createTopic(TopicRequestDTO.builder().name("custom.events").partitions(3).build()))
                .isInstanceOf(BrokerUnavailableException.class);
    }

    @Test
    void deleteTopicProtegeRecursosCoreYMapea404() {
        assertThatThrownBy(() -> service.deleteTopic("workorders.events"))
                .isInstanceOf(ProtectedResourceException.class);
        assertThatThrownBy(() -> service.deleteTopic("workorders.events.DLT-report"))
                .isInstanceOf(ProtectedResourceException.class);
        assertThatThrownBy(() -> service.deleteTopic("audit.timeline"))
                .isInstanceOf(ProtectedResourceException.class);
        verify(adminClient, never()).deleteTopics(anyCollection());

        DeleteTopicsResult notFoundResult = mock(DeleteTopicsResult.class);
        when(notFoundResult.all()).thenReturn(failedFuture(new UnknownTopicOrPartitionException("missing")));
        when(adminClient.deleteTopics(List.of("ghost.topic"))).thenReturn(notFoundResult);

        assertThatThrownBy(() -> service.deleteTopic("ghost.topic"))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void provisionCoreTopicsEsIdempotenteYCreaSoloLosFaltantes() {
        ListTopicsResult listResult = mock(ListTopicsResult.class);
        when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of("workorders.events")));
        when(adminClient.listTopics(any(ListTopicsOptions.class))).thenReturn(listResult);

        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        when(createResult.values()).thenReturn(Map.of("audit.timeline", KafkaFuture.completedFuture(null)));
        when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

        KafkaProvisionResultDTO result = service.provisionCoreTopics();

        assertThat(result.isCreated()).isTrue();
        assertThat(result.getExistingTopics()).containsExactly("workorders.events");
        assertThat(result.getCreatedTopics()).containsExactly("audit.timeline");
        assertThat(result.getTopics()).containsExactly("workorders.events", "audit.timeline");
    }
}
