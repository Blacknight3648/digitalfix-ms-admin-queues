package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.ExchangeRequestDTO;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

import static df.digitalfix_ms_admin_queues.dto.ReglasNombresAmqp.*;

@RestController
@Tag(name = "Exchanges", description = "Crear y eliminar exchanges direct, topic, fanout o headers")
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
    public ResponseEntity<Map<String, String>> deleteExchange(@PathVariable("name") @Pattern(regexp = NOMBRE, message = "El nombre " + NOMBRE_MENSAJE) @Size(max = LARGO_MAXIMO) String exchangeName) {
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
