package df.digitalfix_ms_admin_queues.messaging;

import com.rabbitmq.client.Channel;
import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties.Flujo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

/**
 * Consumidor de la cola de trabajo de un flujo: entrega cada mensaje al microservicio de destino
 * por HTTP y confirma con ACK manual solo si el destino respondió 2xx. Si falla, la decisión
 * (reintento o DLQ) es de {@link ManejadorFallosEntrega}.
 */
@Slf4j
public class ReenviadorMensajes implements ChannelAwareMessageListener {

    private final String nombreFlujo;
    private final Flujo flujo;
    private final ClienteEntregaHttp cliente;
    private final ManejadorFallosEntrega manejadorFallos;

    public ReenviadorMensajes(String nombreFlujo, Flujo flujo, ClienteEntregaHttp cliente, ManejadorFallosEntrega manejadorFallos) {
        this.nombreFlujo = nombreFlujo;
        this.flujo = flujo;
        this.cliente = cliente;
        this.manejadorFallos = manejadorFallos;
    }

    @Override
    public void onMessage(Message mensaje, Channel canal) throws Exception {
        MessageProperties props = mensaje.getMessageProperties();
        long deliveryTag = props.getDeliveryTag();
        log.info("[{}] Mensaje recibido vía {} ({}) | eventId: {} | correlación: {} → {}",
                nombreFlujo, props.getReceivedExchange(), props.getReceivedRoutingKey(),
                props.getMessageId(), props.getCorrelationId(), flujo.destino());

        try {
            cliente.entregar(flujo.destino(), mensaje);
            canal.basicAck(deliveryTag, false);
            log.info("[{}] ACK: eventId {} procesado por el destino", nombreFlujo, props.getMessageId());
        } catch (Exception e) {
            log.error("[{}] Falló la entrega del eventId {}: {}", nombreFlujo, props.getMessageId(), e.getMessage());
            manejadorFallos.resolver(mensaje, canal, deliveryTag, e);
        }
    }
}
