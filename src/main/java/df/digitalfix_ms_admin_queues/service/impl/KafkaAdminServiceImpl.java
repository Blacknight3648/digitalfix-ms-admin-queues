package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.config.KafkaTopologyProperties;
import df.digitalfix_ms_admin_queues.dto.KafkaProvisionResultDTO;
import df.digitalfix_ms_admin_queues.dto.TopicDetailDTO;
import df.digitalfix_ms_admin_queues.dto.TopicRequestDTO;
import df.digitalfix_ms_admin_queues.dto.TopicSummaryDTO;
import df.digitalfix_ms_admin_queues.exception.AdminQueueException;
import df.digitalfix_ms_admin_queues.exception.BrokerUnavailableException;
import df.digitalfix_ms_admin_queues.exception.ResourceAlreadyExistsException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import df.digitalfix_ms_admin_queues.service.KafkaAdminService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaAdminServiceImpl implements KafkaAdminService {

    private static final int MIN_PARTITIONS = 3;

    private final AdminClient adminClient;
    private final KafkaTopologyProperties kafkaProperties;

    @Value("${app.admin.protected-resources:}")
    private List<String> protectedResources = List.of();

    @Override
    public List<TopicSummaryDTO> listTopics() {
        log.info("Listando tópicos de negocio en Apache Kafka");
        try {
            Set<String> topicNames = adminClient.listTopics(new ListTopicsOptions().listInternal(false))
                    .names()
                    .get();

            List<String> businessTopics = topicNames.stream()
                    .filter(name -> !name.startsWith("__"))
                    .sorted()
                    .toList();

            if (businessTopics.isEmpty()) {
                return List.of();
            }

            Map<String, TopicDescription> descriptions = adminClient.describeTopics(businessTopics)
                    .allTopicNames()
                    .get();

            return businessTopics.stream()
                    .map(descriptions::get)
                    .filter(desc -> desc != null && !desc.isInternal())
                    .map(this::toSummary)
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminQueueException("Operación interrumpida al listar tópicos de Kafka", e);
        } catch (ExecutionException e) {
            throw translateKafkaException(e.getCause(), "Error al listar los tópicos de Kafka");
        } catch (Exception e) {
            throw translateKafkaException(e, "Error al listar los tópicos de Kafka");
        }
    }

    @Override
    public TopicDetailDTO getTopicDetails(String name) {
        log.info("Consultando detalle del tópico Kafka: {}", name);
        try {
            TopicDescription description = adminClient.describeTopics(List.of(name))
                    .topicNameValues()
                    .get(name)
                    .get();

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, name);
            Config config = adminClient.describeConfigs(List.of(configResource))
                    .values()
                    .get(configResource)
                    .get();

            return toDetail(description, config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminQueueException("Operación interrumpida al consultar el tópico '" + name + "'", e);
        } catch (ExecutionException e) {
            throw translateKafkaException(e.getCause(), "Error al consultar el tópico '" + name + "'");
        } catch (Exception e) {
            throw translateKafkaException(e, "Error al consultar el tópico '" + name + "'");
        }
    }

    @Override
    public TopicSummaryDTO createTopic(TopicRequestDTO request) {
        log.info("Iniciando creación de tópico Kafka: {}", request.getName());
        if (request.getPartitions() == null || request.getPartitions() < MIN_PARTITIONS) {
            throw new IllegalArgumentException("El número mínimo de particiones es " + MIN_PARTITIONS);
        }

        int effectiveReplicas = request.getReplicationFactor() != null
                ? request.getReplicationFactor()
                : kafkaProperties.getDefaultReplicas();

        NewTopic newTopic = new NewTopic(request.getName(), request.getPartitions(), (short) effectiveReplicas);
        if (request.getConfigs() != null && !request.getConfigs().isEmpty()) {
            newTopic.configs(request.getConfigs());
        }

        try {
            adminClient.createTopics(List.of(newTopic)).all().get();
            log.info("Tópico Kafka '{}' creado (particiones={}, réplicas={})",
                    request.getName(), request.getPartitions(), effectiveReplicas);
            return TopicSummaryDTO.builder()
                    .name(request.getName())
                    .partitions(request.getPartitions())
                    .replicationFactor(effectiveReplicas)
                    .internal(false)
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminQueueException("Operación interrumpida al crear el tópico '" + request.getName() + "'", e);
        } catch (ExecutionException e) {
            throw translateKafkaException(e.getCause(), "Fallo al crear el tópico '" + request.getName() + "'");
        } catch (Exception e) {
            throw translateKafkaException(e, "Fallo al crear el tópico '" + request.getName() + "'");
        }
    }

    @Override
    public void deleteTopic(String name) {
        log.info("Eliminando tópico Kafka: {}", name);
        ProtectedResourceGuard.ensureNotProtected(name, protectedResources, "Kafka");

        try {
            adminClient.deleteTopics(List.of(name)).all().get();
            log.info("Tópico Kafka '{}' eliminado exitosamente", name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminQueueException("Operación interrumpida al eliminar el tópico '" + name + "'", e);
        } catch (ExecutionException e) {
            throw translateKafkaException(e.getCause(), "Fallo al eliminar el tópico '" + name + "'");
        } catch (Exception e) {
            throw translateKafkaException(e, "Fallo al eliminar el tópico '" + name + "'");
        }
    }

    @Override
    public KafkaProvisionResultDTO provisionCoreTopics() {
        log.info("Iniciando aprovisionamiento idempotente de tópicos core en Apache Kafka");
        List<KafkaTopologyProperties.TopicDefinition> definitions = kafkaProperties.getTopics();
        if (definitions == null || definitions.isEmpty()) {
            return KafkaProvisionResultDTO.builder()
                    .created(false)
                    .message("No hay tópicos core configurados para aprovisionar.")
                    .createdTopics(List.of())
                    .existingTopics(List.of())
                    .topics(List.of())
                    .build();
        }

        try {
            Set<String> existingInCluster = adminClient.listTopics(new ListTopicsOptions().listInternal(false))
                    .names()
                    .get();

            List<String> existingTopics = new ArrayList<>();
            List<NewTopic> toCreate = new ArrayList<>();
            List<String> allTopics = new ArrayList<>();

            for (KafkaTopologyProperties.TopicDefinition def : definitions) {
                allTopics.add(def.getName());
                if (existingInCluster.contains(def.getName())) {
                    existingTopics.add(def.getName());
                    continue;
                }
                int partitions = Math.max(MIN_PARTITIONS, def.getPartitions());
                int replicas = def.getReplicas() != null
                        ? def.getReplicas()
                        : kafkaProperties.getDefaultReplicas();
                NewTopic newTopic = new NewTopic(def.getName(), partitions, (short) replicas);
                if (def.getConfigs() != null && !def.getConfigs().isEmpty()) {
                    newTopic.configs(def.getConfigs());
                }
                toCreate.add(newTopic);
            }

            List<String> createdTopics = new ArrayList<>();
            if (!toCreate.isEmpty()) {
                CreateTopicsResult createResult = adminClient.createTopics(toCreate);
                for (Map.Entry<String, KafkaFuture<Void>> entry : createResult.values().entrySet()) {
                    String topicName = entry.getKey();
                    try {
                        entry.getValue().get();
                        createdTopics.add(topicName);
                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof TopicExistsException) {
                            existingTopics.add(topicName);
                        } else {
                            throw translateKafkaException(ex.getCause(),
                                    "Fallo al aprovisionar el tópico core '" + topicName + "'");
                        }
                    }
                }
            }

            boolean anyCreated = !createdTopics.isEmpty();
            String message = anyCreated
                    ? "Tópicos de Kafka aprovisionados exitosamente."
                    : "Todos los tópicos core de Kafka ya existían.";
            log.info("{} (creados={}, existentes={})", message, createdTopics.size(), existingTopics.size());

            return KafkaProvisionResultDTO.builder()
                    .created(anyCreated)
                    .message(message)
                    .createdTopics(createdTopics)
                    .existingTopics(existingTopics)
                    .topics(allTopics)
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminQueueException("Operación interrumpida al aprovisionar los tópicos core de Kafka", e);
        } catch (ExecutionException e) {
            throw translateKafkaException(e.getCause(), "Fallo al aprovisionar los tópicos core de Kafka");
        } catch (RuntimeException e) {
            if (e instanceof AdminQueueException) {
                throw e;
            }
            throw translateKafkaException(e, "Fallo al aprovisionar los tópicos core de Kafka");
        }
    }

    private TopicSummaryDTO toSummary(TopicDescription desc) {
        int replicationFactor = desc.partitions().isEmpty()
                ? 0
                : desc.partitions().get(0).replicas().size();
        return TopicSummaryDTO.builder()
                .name(desc.name())
                .partitions(desc.partitions().size())
                .replicationFactor(replicationFactor)
                .internal(desc.isInternal())
                .build();
    }

    private TopicDetailDTO toDetail(TopicDescription desc, Config config) {
        int replicationFactor = desc.partitions().isEmpty()
                ? 0
                : desc.partitions().get(0).replicas().size();

        List<TopicDetailDTO.PartitionInfoDTO> partitionDetails = desc.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> TopicDetailDTO.PartitionInfoDTO.builder()
                        .partition(p.partition())
                        .leader(p.leader() != null ? p.leader().id() : -1)
                        .replicas(p.replicas().stream().map(Node::id).toList())
                        .isr(p.isr().stream().map(Node::id).toList())
                        .build())
                .toList();

        Map<String, String> dynamicOrExplicitConfigs = new TreeMap<>();
        if (config != null) {
            for (ConfigEntry entry : config.entries()) {
                if (entry.value() != null
                        && entry.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG) {
                    dynamicOrExplicitConfigs.put(entry.name(), entry.value());
                }
            }
        }

        return TopicDetailDTO.builder()
                .name(desc.name())
                .partitions(desc.partitions().size())
                .replicationFactor(replicationFactor)
                .internal(desc.isInternal())
                .partitionDetails(partitionDetails)
                .configs(dynamicOrExplicitConfigs)
                .build();
    }

    private RuntimeException translateKafkaException(Throwable cause, String context) {
        Throwable root = cause != null ? cause : new RuntimeException(context);
        if (root instanceof TopicExistsException) {
            return new ResourceAlreadyExistsException(context + ": el tópico ya existe en el clúster");
        }
        if (root instanceof UnknownTopicOrPartitionException) {
            return new ResourceNotFoundException(context + ": el tópico especificado no existe en Kafka");
        }
        if (root instanceof InvalidReplicationFactorException
                || root instanceof InvalidPartitionsException
                || root instanceof InvalidConfigurationException
                || root instanceof InvalidTopicException
                || root instanceof InvalidRequestException) {
            return new IllegalArgumentException(context + ": " + root.getMessage(), root);
        }
        if (root instanceof TimeoutException
                || root instanceof DisconnectException
                || root instanceof NetworkException
                || root instanceof RetriableException
                || root instanceof java.util.concurrent.TimeoutException) {
            return new BrokerUnavailableException("El clúster de Kafka no responde: " + root.getMessage(), root);
        }
        return new AdminQueueException(context, root);
    }
}
