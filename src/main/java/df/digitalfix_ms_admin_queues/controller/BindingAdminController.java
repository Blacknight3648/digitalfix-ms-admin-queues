package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

import static df.digitalfix_ms_admin_queues.dto.ReglasNombresAmqp.*;

@RestController
@Tag(name = "Bindings", description = "Vincular y desvincular colas de exchanges mediante una routing key")
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
     * DELETE /api/v1/bindings?exchange=cmd.topic&amp;queue=q.demo&amp;routingKey=demo.*
     * Elimina la vinculación entre una cola y un exchange. Un binding no tiene id propio: lo
     * identifican los tres valores, que van como query params (en la URL, '#' se envía como %23).
     */
    @DeleteMapping
    public ResponseEntity<Map<String, String>> removeBinding(
            @RequestParam("exchange") @NotBlank @Pattern(regexp = NOMBRE, message = "El exchange " + NOMBRE_MENSAJE) String exchange,
            @RequestParam("queue") @NotBlank @Pattern(regexp = NOMBRE, message = "La cola " + NOMBRE_MENSAJE) String queue,
            @RequestParam("routingKey") @NotBlank @Pattern(regexp = ROUTING_KEY, message = "La routing key " + ROUTING_KEY_MENSAJE) String routingKey) {
        BindingRequestDTO bindingRequestDTO = BindingRequestDTO.builder()
                .exchangeName(exchange)
                .queueName(queue)
                .routingKey(routingKey)
                .build();
        rabbitAdminService.removeBinding(bindingRequestDTO);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Binding eliminado exitosamente.");
        return ResponseEntity.ok(response);
    }
}
