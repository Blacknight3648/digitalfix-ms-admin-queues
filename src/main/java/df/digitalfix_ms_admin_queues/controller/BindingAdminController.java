package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/bindings")
@RequiredArgsConstructor
public class BindingAdminController {

    private final RabbitAdminService rabbitAdminService;

    /**
     * POST /api/v1/bindings
     * Vincula una cola a un exchange mediante una clave de enrutamiento (routingKey).
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> declareBinding(@Valid @RequestBody BindingRequestDTO bindingRequestDTO) {
        rabbitAdminService.declareBinding(bindingRequestDTO);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Binding creado exitosamente entre cola '" + bindingRequestDTO.getQueueName() + 
                "' y exchange '" + bindingRequestDTO.getExchangeName() + "' con clave '" + bindingRequestDTO.getRoutingKey() + "'.");
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * DELETE /api/v1/bindings
     * Elimina la vinculación entre una cola y un exchange.
     */
    @DeleteMapping
    public ResponseEntity<Map<String, String>> removeBinding(@Valid @RequestBody BindingRequestDTO bindingRequestDTO) {
        rabbitAdminService.removeBinding(bindingRequestDTO);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Binding eliminado exitosamente.");
        return ResponseEntity.ok(response);
    }
}
