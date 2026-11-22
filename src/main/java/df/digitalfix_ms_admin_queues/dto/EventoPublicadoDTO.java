package df.digitalfix_ms_admin_queues.dto;

/** Respuesta de la publicación: el broker ya confirmó el mensaje y alguna cola lo recibió. */
public record EventoPublicadoDTO(String eventId, String exchange, String routingKey) {
}
