package df.digitalfix_ms_admin_queues.messaging;

import com.rabbitmq.client.Channel;
import df.digitalfix_ms_admin_queues.config.TopologiaDePrueba;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

// Política de errores de la entrega: 4xx del destino → NACK sin requeue (DLQ); 5xx o sin respuesta →
// reintento con límite; agotado → DLQ. Nunca requeue=true (bucle infinito).
class ManejadorFallosEntregaTest {

    private static final long TAG = 42L;
    private static final String COLA = "q.cmd.email";

    private RabbitTemplate rabbitTemplate;
    private Channel canal;
    private ManejadorFallosEntrega manejador;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        canal = mock(Channel.class);
        manejador = new ManejadorFallosEntrega(rabbitTemplate, TopologiaDePrueba.cargar());
    }

    private static Message mensaje(Integer reintentosPrevios) {
        MessageProperties props = new MessageProperties();
        props.setConsumerQueue(COLA);
        if (reintentosPrevios != null) {
            props.setHeader(ManejadorFallosEntrega.HEADER_REINTENTOS, reintentosPrevios);
        }
        return new Message("{}".getBytes(), props);
    }

    @Test
    void payloadRechazadoPorElDestinoVaDirectoALaDlq() throws IOException {
        manejador.resolver(mensaje(null), canal, TAG,
                HttpClientErrorException.create(HttpStatus.BAD_REQUEST, "Bad Request", null, null, null));

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void plantillaInexistenteEnElDestinoTambienEsDefinitivo() throws IOException {
        manejador.resolver(mensaje(null), canal, TAG,
                HttpClientErrorException.create(HttpStatus.UNPROCESSABLE_CONTENT, "Unprocessable", null, null, null));

        verify(canal).basicNack(TAG, false, false);
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void demasiadasPeticionesSeReintenta() {
        assertThat(ManejadorFallosEntrega.esDefinitivo(
                HttpClientErrorException.create(HttpStatus.TOO_MANY_REQUESTS, "Too Many", null, null, null))).isFalse();
    }

    @Test
    void destinoCaidoSeReencolaConElContadorIncrementadoYAckDelOriginal() throws IOException {
        manejador.resolver(mensaje(1), canal, TAG, new ResourceAccessException("Connection refused"));

        ArgumentCaptor<Message> copia = ArgumentCaptor.forClass(Message.class);
        verify(rabbitTemplate).send(eq(""), eq(COLA), copia.capture());
        assertThat((Integer) copia.getValue().getMessageProperties().getHeader(ManejadorFallosEntrega.HEADER_REINTENTOS))
                .isEqualTo(2);
        verify(canal).basicAck(TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void errorDelServidorDeDestinoEsTransitorio() throws IOException {
        manejador.resolver(mensaje(null), canal, TAG,
                HttpServerErrorException.create(HttpStatus.SERVICE_UNAVAILABLE, "SMTP caído", null, null, null));

        verify(rabbitTemplate).send(eq(""), eq(COLA), any(Message.class));
        verify(canal).basicAck(TAG, false);
    }

    @Test
    void reintentosAgotadosTerminanEnLaDlq() throws IOException {
        manejador.resolver(mensaje(3), canal, TAG, new ResourceAccessException("timeout"));

        verify(canal).basicNack(TAG, false, false);
        verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class));
    }
}
