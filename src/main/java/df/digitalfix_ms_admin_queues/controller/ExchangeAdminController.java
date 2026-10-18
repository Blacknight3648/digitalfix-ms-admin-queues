package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.ExchangeRequestDTO;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/exchanges")
@RequiredArgsConstructor
public class ExchangeAdminController {

    private final RabbitAdminService rabbitAdminService;

    /**
     * POST /api/v1/exchanges
     * Declara un nuevo Exchange (direct, topic, fanout, headers).
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> createExchange(@Valid @RequestBody ExchangeRequestDTO exchangeRequestDTO) {
        rabbitAdminService.createExchange(exchangeRequestDTO);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Exchange '" + exchangeRequestDTO.getExchangeName() + "' (" + exchangeRequestDTO.getType() + ") creado exitosamente.");
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * DELETE /api/v1/exchanges/{name}
     * Elimina un Exchange de RabbitMQ.
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<Map<String, String>> deleteExchange(@PathVariable("name") String exchangeName) {
        boolean deleted = rabbitAdminService.deleteExchange(exchangeName);
        Map<String, String> response = new HashMap<>();
        if (deleted) {
            response.put("message", "Exchange '" + exchangeName + "' eliminado exitosamente.");
            return ResponseEntity.ok(response);
        } else {
            response.put("message", "No se pudo eliminar el exchange '" + exchangeName + "'.");
            return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
        }
    }
}
