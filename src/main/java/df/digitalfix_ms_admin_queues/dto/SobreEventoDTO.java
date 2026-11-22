package df.digitalfix_ms_admin_queues.dto;

import tools.jackson.databind.JsonNode;

import java.time.LocalDateTime;

/**
 * Sobre común de todos los mensajes del caso: es el JSON que viaja por RabbitMQ y el que recibe el
 * microservicio de destino ({@code EventEnvelopeDTO} en ms-notify).
 */
public record SobreEventoDTO(
        String eventId,
        String eventType,
        LocalDateTime timestamp,
        String traceId,
        String correlationId,
        JsonNode data
) {
}
