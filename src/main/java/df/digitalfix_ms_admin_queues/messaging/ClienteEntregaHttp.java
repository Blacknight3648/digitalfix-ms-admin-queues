package df.digitalfix_ms_admin_queues.messaging;

import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Inyecta un mensaje de la cola en el microservicio de destino: POST del cuerpo JSON tal cual
 * llegó (el sobre común) al endpoint interno configurado en el flujo. ms-admin-queues no interpreta
 * el contenido de negocio; la validación y el procesamiento son del destino.
 * <p>
 * Los headers permiten rastrear el mensaje en el log del destino y detectar reentregas.
 * Cualquier error (4xx, 5xx, timeout) se propaga como excepción de RestClient y lo clasifica
 * {@link ManejadorFallosEntrega}.
 */
@Component
public class ClienteEntregaHttp {

    public static final String HEADER_EVENT_ID = "X-Event-Id";
    public static final String HEADER_CORRELATION_ID = "X-Correlation-Id";
    public static final String HEADER_COLA_ORIGEN = "X-Source-Queue";
    public static final String HEADER_REINTENTO = "X-Retry-Count";

    private final RestClient restClient;

    @Autowired
    public ClienteEntregaHttp(TopologiaRabbitProperties topologia) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(topologia.entrega().connectTimeout());
        factory.setReadTimeout(topologia.entrega().readTimeout());
        this.restClient = RestClient.builder().requestFactory(factory).build();
    }

    ClienteEntregaHttp(RestClient restClient) {
        this.restClient = restClient;
    }

    public void entregar(String destino, Message mensaje) {
        MessageProperties props = mensaje.getMessageProperties();
        restClient.post()
                .uri(destino)
                .contentType(MediaType.APPLICATION_JSON)
                .headers(h -> {
                    if (props.getMessageId() != null) {
                        h.set(HEADER_EVENT_ID, props.getMessageId());
                    }
                    if (props.getCorrelationId() != null) {
                        h.set(HEADER_CORRELATION_ID, props.getCorrelationId());
                    }
                    if (props.getConsumerQueue() != null) {
                        h.set(HEADER_COLA_ORIGEN, props.getConsumerQueue());
                    }
                    h.set(HEADER_REINTENTO, String.valueOf(ManejadorFallosEntrega.intentosPrevios(mensaje)));
                })
                .body(mensaje.getBody())
                .retrieve()
                .toBodilessEntity();
    }
}
