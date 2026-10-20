package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.QueueRequestDTO;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import jakarta.validation.Valid;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RestController
@Tag(name = "Colas", description = "Crear, consultar, purgar y eliminar colas (con DLX y TTL opcionales)")
@RequestMapping("/api/v1/queues")
@RequiredArgsConstructor
public class QueueAdminController {

    private final RabbitAdminService rabbitAdminService;

    /**
     * POST /api/v1/queues
     * Crea una nueva cola en RabbitMQ con soporte de DLX opcional.
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> createQueue(@Valid @RequestBody QueueRequestDTO queueRequestDTO) {
        rabbitAdminService.createQueue(queueRequestDTO);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Cola '" + queueRequestDTO.getQueueName() + "' creada exitosamente.");
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * GET /api/v1/queues/{name}
     * Consulta el estado y métricas de una cola existente.
     */
    @GetMapping("/{name}")
    public ResponseEntity<Properties> getQueueProperties(@PathVariable("name") String queueName) {
        Properties properties = rabbitAdminService.getQueueProperties(queueName);
        return ResponseEntity.ok(properties);
    }

    /**
     * DELETE /api/v1/queues/{name}
     * Elimina una cola en RabbitMQ.
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<Map<String, String>> deleteQueue(
            @PathVariable("name") String queueName,
            @RequestParam(value = "unusedOnly", defaultValue = "false") boolean unusedOnly,
            @RequestParam(value = "emptyOnly", defaultValue = "false") boolean emptyOnly) {

        boolean deleted = rabbitAdminService.deleteQueue(queueName, unusedOnly, emptyOnly);
        Map<String, String> response = new HashMap<>();
        if (deleted) {
            response.put("message", "Cola '" + queueName + "' eliminada exitosamente.");
            return ResponseEntity.ok(response);
        } else {
            response.put("message", "No se pudo eliminar la cola '" + queueName + "' (posiblemente contiene mensajes o consumidores activos).");
            return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
        }
    }

    /**
     * POST /api/v1/queues/{name}/purge
     * Purga (vacía) todos los mensajes acumulados en una cola.
     */
    @PostMapping("/{name}/purge")
    public ResponseEntity<Map<String, Object>> purgeQueue(@PathVariable("name") String queueName) {
        int purgedCount = rabbitAdminService.purgeQueue(queueName);
        Map<String, Object> response = new HashMap<>();
        response.put("message", "Cola purgada exitosamente.");
        response.put("queueName", queueName);
        response.put("purgedMessagesCount", purgedCount);
        return ResponseEntity.ok(response);
    }
}
