package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties;
import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties.Flujo;
import df.digitalfix_ms_admin_queues.dto.EventoPublicadoDTO;
import df.digitalfix_ms_admin_queues.dto.PublicarEventoRequestDTO;
import df.digitalfix_ms_admin_queues.dto.SobreEventoDTO;
import df.digitalfix_ms_admin_queues.exception.PublicacionFallidaException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import df.digitalfix_ms_admin_queues.service.PublicadorEventosService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica en RabbitMQ los eventos que los productores envían por HTTP.
 * <ul>
 *   <li><b>Enrutamiento</b>: sin subtipo, por {@code cmd.direct} con la routing key del flujo
 *       (dispatch.ticket); con subtipo, por {@code cmd.topic} con {@code <prefijo>.<subtipo>}
 *       (email.asignada, que calza con email.*).</li>
 *   <li><b>Sobre común</b>: eventId, eventType, timestamp, traceId, correlationId y data. El
 *       eventId viaja también como messageId para rastrearlo en el broker y en el destino.</li>
 *   <li><b>Confirmación</b>: se espera el publisher confirm. Si el broker no lo acepta o ninguna
 *       cola lo recibe (mandatory → returned), el productor recibe un error en vez de un 202, así
 *       un evento nunca se pierde en silencio.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PublicadorEventosServiceImpl implements PublicadorEventosService {

    private final RabbitTemplate rabbitTemplate;
    private final TopologiaRabbitProperties topologia;
    private final JsonMapper jsonMapper;

    @Override
    public EventoPublicadoDTO publicar(String nombreFlujo, PublicarEventoRequestDTO evento) {
        Flujo flujo = topologia.flujo(nombreFlujo)
                .orElseThrow(() -> new ResourceNotFoundException("No existe el flujo de mensajería '" + nombreFlujo
                        + "'. Flujos disponibles: " + topologia.flujos().keySet()));

        String exchange;
        String routingKey;
        if (evento.tieneSubtipo()) {
            exchange = topologia.exchanges().topic();
            routingKey = flujo.prefijoTopic() + "." + evento.subtipo().toLowerCase(Locale.ROOT);
        } else {
            exchange = topologia.exchanges().direct();
            routingKey = flujo.routingKey();
        }

        SobreEventoDTO sobre = new SobreEventoDTO(
                UUID.randomUUID().toString(),
                evento.eventType(),
                LocalDateTime.now(),
                evento.traceId() != null && !evento.traceId().isBlank()
                        ? evento.traceId()
                        : "TRACE-" + UUID.randomUUID().toString().substring(0, 8),
                evento.correlationId(),
                evento.data());

        Message mensaje = new Message(jsonMapper.writeValueAsBytes(sobre), propiedades(sobre));
        CorrelationData correlacion = new CorrelationData(sobre.eventId());
        try {
            rabbitTemplate.send(exchange, routingKey, mensaje, correlacion);
        } catch (AmqpException e) {
            log.error("No se pudo publicar el evento {} en {} ({}): {}", sobre.eventId(), exchange, routingKey, e.getMessage());
            throw new PublicacionFallidaException("El broker de mensajería no está disponible", e);
        }
        esperarConfirmacion(correlacion, exchange, routingKey);

        log.info("Evento {} ({}) de {} publicado en {} con routingKey {}",
                sobre.eventId(), sobre.eventType(), sobre.correlationId(), exchange, routingKey);
        return new EventoPublicadoDTO(sobre.eventId(), exchange, routingKey);
    }

    private static MessageProperties propiedades(SobreEventoDTO sobre) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setContentEncoding("UTF-8");
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setMessageId(sobre.eventId());
        props.setCorrelationId(sobre.correlationId());
        props.setType(sobre.eventType());
        props.setTimestamp(new Date());
        return props;
    }

    private void esperarConfirmacion(CorrelationData correlacion, String exchange, String routingKey) {
        String eventId = correlacion.getId();
        CorrelationData.Confirm confirmacion;
        try {
            confirmacion = correlacion.getFuture()
                    .get(topologia.publicacion().confirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublicacionFallidaException("Se interrumpió la espera de confirmación del evento " + eventId, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("El broker no confirmó el evento {} a tiempo", eventId);
            throw new PublicacionFallidaException("El broker no confirmó el evento " + eventId + " a tiempo", e);
        }

        if (!confirmacion.ack()) {
            log.error("El broker RECHAZÓ el evento {}: {}", eventId, confirmacion.reason());
            throw new PublicacionFallidaException("El broker rechazó el evento " + eventId + ": " + confirmacion.reason());
        }
        // Con mandatory, el retorno se registra en la correlación antes de completar la confirmación.
        if (correlacion.getReturned() != null) {
            log.error("Evento {} NO ENTREGADO: ninguna cola recibe exchange='{}' routingKey='{}' ({})",
                    eventId, exchange, routingKey, correlacion.getReturned().getReplyText());
            throw new PublicacionFallidaException("Ninguna cola recibe el evento (exchange '" + exchange
                    + "', routing key '" + routingKey + "'). Revise los bindings del flujo.");
        }
    }
}
