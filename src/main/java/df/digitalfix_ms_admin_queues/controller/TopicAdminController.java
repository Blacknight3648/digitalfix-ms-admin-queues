package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.TopicDetailDTO;
import df.digitalfix_ms_admin_queues.dto.TopicRequestDTO;
import df.digitalfix_ms_admin_queues.dto.TopicSummaryDTO;
import df.digitalfix_ms_admin_queues.service.KafkaAdminService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static df.digitalfix_ms_admin_queues.dto.ReglasNombresKafka.LARGO_MAXIMO;
import static df.digitalfix_ms_admin_queues.dto.ReglasNombresKafka.NOMBRE;
import static df.digitalfix_ms_admin_queues.dto.ReglasNombresKafka.NOMBRE_MENSAJE;

@RestController
@Tag(name = "Tópicos Kafka", description = "Listar, consultar detalles, crear y eliminar tópicos en Apache Kafka")
@RequestMapping("/api/v1/topics")
@RequiredArgsConstructor
public class TopicAdminController {

    private final KafkaAdminService kafkaAdminService;

    /**
     * GET /api/v1/topics
     * Lista los tópicos de negocio activos en Apache Kafka (excluye tópicos internos como __consumer_offsets).
     */
    @GetMapping
    public ResponseEntity<List<TopicSummaryDTO>> listTopics() {
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    /**
     * GET /api/v1/topics/{name}
     * Consulta particiones, factor de réplica y configuraciones de un tópico existente.
     */
    @GetMapping("/{name}")
    public ResponseEntity<TopicDetailDTO> getTopicDetails(
            @PathVariable("name") @Pattern(regexp = NOMBRE, message = "El nombre del tópico " + NOMBRE_MENSAJE) @Size(max = LARGO_MAXIMO) String name) {
        return ResponseEntity.ok(kafkaAdminService.getTopicDetails(name));
    }

    /**
     * POST /api/v1/topics
     * Crea un tópico en Apache Kafka mediante AdminClient.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createTopic(@Valid @RequestBody TopicRequestDTO topicRequestDTO) {
        TopicSummaryDTO created = kafkaAdminService.createTopic(topicRequestDTO);
        Map<String, Object> response = new HashMap<>();
        response.put("message", "Tópico '" + created.getName() + "' creado exitosamente.");
        response.put("name", created.getName());
        response.put("partitions", created.getPartitions());
        response.put("replicationFactor", created.getReplicationFactor());
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * DELETE /api/v1/topics/{name}
     * Elimina un tópico no protegido en Apache Kafka.
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<Map<String, String>> deleteTopic(
            @PathVariable("name") @Pattern(regexp = NOMBRE, message = "El nombre del tópico " + NOMBRE_MENSAJE) @Size(max = LARGO_MAXIMO) String name) {
        kafkaAdminService.deleteTopic(name);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Tópico '" + name + "' eliminado exitosamente.");
        return ResponseEntity.ok(response);
    }
}
