package df.digitalfix_ms_admin_queues.messaging;

import com.rabbitmq.client.Channel;
import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties.Flujo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

// Inyección de los mensajes en el microservicio de destino: el cuerpo viaja tal cual llegó a la
// cola y el ACK depende de la respuesta HTTP del destino.
class ReenviadorMensajesTest {

    private static final String DESTINO = "http://ms-notify/internal/v1/notificaciones/email";
    private static final String SOBRE = "{\"eventId\":\"evt-1\",\"eventType\":\"WORK_ORDER_STATUS_CHANGED\",\"data\":{\"workOrderId\":15}}";
    private static final long TAG = 7L;

    private MockRestServiceServer destino;
    private RabbitTemplate rabbitTemplate;
    private Channel canal;
    private ReenviadorMensajes reenviador;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        destino = MockRestServiceServer.bindTo(builder).build();
        rabbitTemplate = mock(RabbitTemplate.class);
        canal = mock(Channel.class);
        Flujo flujo = new Flujo("q.cmd.email", "q.cmd.email.dlq", "email.send", "email.*", "email.dead", DESTINO);
        ManejadorFallosEntrega manejador = new ManejadorFallosEntrega(rabbitTemplate,
                df.digitalfix_ms_admin_queues.config.TopologiaDePrueba.cargar());
        reenviador = new ReenviadorMensajes("email", flujo, new ClienteEntregaHttp(builder.build()), manejador);
    }

    private static Message mensaje() {
        MessageProperties props = new MessageProperties();
        props.setConsumerQueue("q.cmd.email");
        props.setDeliveryTag(TAG);
        props.setMessageId("evt-1");
        props.setCorrelationId("OT-15");
        return new Message(SOBRE.getBytes(StandardCharsets.UTF_8), props);
    }

    @Test
    void entregaElSobreTalCualYConfirmaConAck() throws Exception {
        destino.expect(requestTo(DESTINO))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().string(SOBRE))
                .andExpect(header(ClienteEntregaHttp.HEADER_EVENT_ID, "evt-1"))
                .andExpect(header(ClienteEntregaHttp.HEADER_CORRELATION_ID, "OT-15"))
                .andExpect(header(ClienteEntregaHttp.HEADER_COLA_ORIGEN, "q.cmd.email"))
                .andExpect(header(ClienteEntregaHttp.HEADER_REINTENTO, "0"))
                .andRespond(withSuccess());

        reenviador.onMessage(mensaje(), canal);

        destino.verify();
        verify(canal).basicAck(TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void siElDestinoRechazaElPayloadVaALaDlq() throws Exception {
        destino.expect(requestTo(DESTINO)).andRespond(withStatus(HttpStatus.BAD_REQUEST));

        reenviador.onMessage(mensaje(), canal);

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void siElDestinoFallaSeReintenta() throws Exception {
        destino.expect(requestTo(DESTINO)).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        reenviador.onMessage(mensaje(), canal);

        verify(rabbitTemplate).send(eq(""), eq("q.cmd.email"), any(Message.class));
        verify(canal).basicAck(TAG, false);
    }
}
