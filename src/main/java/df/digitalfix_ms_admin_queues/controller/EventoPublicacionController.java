package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.EventoPublicadoDTO;
import df.digitalfix_ms_admin_queues.dto.PublicarEventoRequestDTO;
import df.digitalfix_ms_admin_queues.service.PublicadorEventosService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * API interna de publicación: los microservicios productores (ms-workorders) envían aquí sus
 * eventos en vez de conectarse a RabbitMQ. Va bajo {@code /internal}: el BFF no la enruta, solo es
 * accesible dentro de la red digitalfix-net.
 */
@RestController
@Tag(name = "Publicación de eventos (interna)", description = "Los productores publican eventos sin conocer exchanges ni routing keys")
@RequestMapping("/internal/v1/eventos")
@RequiredArgsConstructor
public class EventoPublicacionController {

    private final PublicadorEventosService publicador;

    /**
     * POST /internal/v1/eventos/{flujo}
     * Publica un evento en el flujo indicado (email, dispatch, report). Responde 202 cuando el
     * broker ya confirmó el mensaje; 404 si el flujo no existe; 503 si el broker no lo aceptó.
     */
    @PostMapping("/{flujo}")
    public ResponseEntity<EventoPublicadoDTO> publicar(
            @PathVariable("flujo") @Pattern(regexp = "^[a-z0-9_-]+$", message = "El flujo solo puede contener minúsculas, números, guiones y guiones bajos") String flujo,
            @Valid @RequestBody PublicarEventoRequestDTO evento) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(publicador.publicar(flujo, evento));
    }
}
