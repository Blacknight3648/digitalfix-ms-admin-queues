package df.digitalfix_ms_admin_queues.service;

import df.digitalfix_ms_admin_queues.dto.EventoPublicadoDTO;
import df.digitalfix_ms_admin_queues.dto.PublicarEventoRequestDTO;

/** Publicación centralizada: los productores entregan eventos por HTTP y este servicio los encola. */
public interface PublicadorEventosService {

    /**
     * Publica el evento en el flujo indicado y espera la confirmación del broker.
     *
     * @throws df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException si el flujo no existe
     * @throws df.digitalfix_ms_admin_queues.exception.PublicacionFallidaException si el broker no lo aceptó
     */
    EventoPublicadoDTO publicar(String flujo, PublicarEventoRequestDTO evento);
}
