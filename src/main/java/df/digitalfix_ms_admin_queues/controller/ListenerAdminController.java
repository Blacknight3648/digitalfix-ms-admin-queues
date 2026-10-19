package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.ListenerContainerDTO;
import df.digitalfix_ms_admin_queues.service.ListenerAdminService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/listeners")
@RequiredArgsConstructor
public class ListenerAdminController {

    private final ListenerAdminService listenerAdminService;

    /**
     * GET /api/v1/listeners
     * Obtiene el listado y estado de todos los listeners registrados en el broker.
     */
    @GetMapping
    public ResponseEntity<List<ListenerContainerDTO>> getAllListeners() {
        List<ListenerContainerDTO> listeners = listenerAdminService.getAllListeners();
        return ResponseEntity.ok(listeners);
    }

    /**
     * GET /api/v1/listeners/{id}
     * Obtiene el estado detallado de un listener específico.
     */
    @GetMapping("/{id}")
    public ResponseEntity<ListenerContainerDTO> getListenerById(@PathVariable("id") String listenerId) {
        ListenerContainerDTO listener = listenerAdminService.getListenerById(listenerId);
        return ResponseEntity.ok(listener);
    }

    /**
     * POST /api/v1/listeners/{id}/start
     * Inicia un listener detenido.
     */
    @PostMapping("/{id}/start")
    public ResponseEntity<Map<String, String>> startListener(@PathVariable("id") String listenerId) {
        listenerAdminService.startListener(listenerId);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Listener '" + listenerId + "' iniciado exitosamente.");
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/v1/listeners/{id}/stop
     * Detiene un listener en ejecución.
     */
    @PostMapping("/{id}/stop")
    public ResponseEntity<Map<String, String>> stopListener(@PathVariable("id") String listenerId) {
        listenerAdminService.stopListener(listenerId);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Listener '" + listenerId + "' detenido exitosamente.");
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/v1/listeners/{id}/pause
     * Pausa temporalmente el consumo del listener sin cerrar la conexión.
     */
    @PostMapping("/{id}/pause")
    public ResponseEntity<Map<String, String>> pauseListener(@PathVariable("id") String listenerId) {
        listenerAdminService.pauseListener(listenerId);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Consumo del listener '" + listenerId + "' pausado exitosamente.");
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/v1/listeners/{id}/resume
     * Reanuda el consumo de un listener pausado.
     */
    @PostMapping("/{id}/resume")
    public ResponseEntity<Map<String, String>> resumeListener(@PathVariable("id") String listenerId) {
        listenerAdminService.resumeListener(listenerId);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Consumo del listener '" + listenerId + "' reanudado exitosamente.");
        return ResponseEntity.ok(response);
    }
}
