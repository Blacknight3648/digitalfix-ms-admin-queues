package df.digitalfix_ms_admin_queues.messaging;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.nio.charset.StandardCharsets;

/**
 * Listener de cuarentena de todas las DLQ: registra en el log el mensaje fallido tal cual llegó
 * (sin interpretarlo) junto con los headers {@code x-death}, y lo confirma. Para conservar los
 * mensajes en la DLQ y revisarlos en la consola de RabbitMQ, se detiene este listener
 * (POST /api/v1/listeners/listener-dlq/stop).
 */
@Slf4j
public class AuditorDlq implements ChannelAwareMessageListener {

    @Override
    public void onMessage(Message mensaje, Channel canal) throws Exception {
        String cola = mensaje.getMessageProperties().getConsumerQueue();
        log.error("[ALERTA DLQ CUARENTENA] Mensaje fallido detectado en la cola muerta '{}'", cola);
        log.error("   -> eventId: {} | correlación: {}", mensaje.getMessageProperties().getMessageId(),
                mensaje.getMessageProperties().getCorrelationId());
        log.error("   -> Payload afectado: {}", new String(mensaje.getBody(), StandardCharsets.UTF_8));
        log.error("   -> Encabezados de error (x-death): {}", mensaje.getMessageProperties().getHeaders().get("x-death"));

        canal.basicAck(mensaje.getMessageProperties().getDeliveryTag(), false);
        log.info("[ALERTA DLQ CUARENTENA] ACK manual emitido para auditoría y desalojo en '{}'", cola);
    }
}
