package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.config.TopologiaDePrueba;
import df.digitalfix_ms_admin_queues.dto.EventoPublicadoDTO;
import df.digitalfix_ms_admin_queues.dto.PublicarEventoRequestDTO;
import df.digitalfix_ms_admin_queues.exception.PublicacionFallidaException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

// Publicación centralizada: el productor no conoce exchanges ni routing keys; el servicio los
// resuelve desde la topología, arma el sobre común y solo responde OK si el broker confirmó.
class PublicadorEventosServiceImplTest {

    private final JsonMapper json = JsonMapper.builder().build();
    private RabbitTemplate rabbitTemplate;
    private PublicadorEventosServiceImpl publicador;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        publicador = new PublicadorEventosServiceImpl(rabbitTemplate, TopologiaDePrueba.cargar(), json);
    }

    private PublicarEventoRequestDTO evento(String subtipo) {
        JsonNode data = json.readTree("{\"workOrderId\":55,\"emailTo\":\"cliente@digitalfix.cl\"}");
        return new PublicarEventoRequestDTO("WORK_ORDER_STATUS_CHANGED", "OT-55", null, subtipo, data);
    }

    /** El broker confirma (ack) cada publicación; opcionalmente la devuelve por no tener cola. */
    private void brokerResponde(boolean ack, boolean devuelto) {
        doAnswer(inv -> {
            CorrelationData correlacion = inv.getArgument(3);
            if (devuelto) {
                correlacion.setReturned(new ReturnedMessage(inv.getArgument(2), 312, "NO_ROUTE",
                        inv.getArgument(0), inv.getArgument(1)));
            }
            correlacion.getFuture().complete(new CorrelationData.Confirm(ack, ack ? null : "nack del broker"));
            return null;
        }).when(rabbitTemplate).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
    }

    private Message mensajePublicado(String exchange, String routingKey) {
        ArgumentCaptor<Message> mensaje = ArgumentCaptor.forClass(Message.class);
        verify(rabbitTemplate).send(eq(exchange), eq(routingKey), mensaje.capture(), any(CorrelationData.class));
        return mensaje.getValue();
    }

    @Test
    void conSubtipoSePublicaPorElTopicConPrefijoPunto() {
        brokerResponde(true, false);

        EventoPublicadoDTO resultado = publicador.publicar("email", evento("ASIGNADA"));

        mensajePublicado("cmd.topic", "email.asignada");
        assertThat(resultado.exchange()).isEqualTo("cmd.topic");
        assertThat(resultado.routingKey()).isEqualTo("email.asignada");
    }

    @Test
    void sinSubtipoSePublicaPorElDirectConLaRoutingKeyDelFlujo() {
        brokerResponde(true, false);

        publicador.publicar("dispatch", evento(null));

        mensajePublicado("cmd.direct", "dispatch.ticket");
    }

    @Test
    void elMensajeLlevaElSobreComunYElEventIdComoMessageId() {
        brokerResponde(true, false);

        EventoPublicadoDTO resultado = publicador.publicar("report", evento(""));

        Message mensaje = mensajePublicado("cmd.direct", "report.gen");
        JsonNode sobre = json.readTree(mensaje.getBody());
        assertThat(sobre.get("eventId").asString()).isEqualTo(resultado.eventId());
        assertThat(sobre.get("eventType").asString()).isEqualTo("WORK_ORDER_STATUS_CHANGED");
        assertThat(sobre.get("correlationId").asString()).isEqualTo("OT-55");
        assertThat(sobre.get("traceId").asString()).startsWith("TRACE-");
        // ms-notify lee la fecha como LocalDateTime ISO-8601, no como número
        assertThat(sobre.get("timestamp").isString()).isTrue();
        assertThat(sobre.get("data").get("workOrderId").asInt()).isEqualTo(55);

        MessageProperties props = mensaje.getMessageProperties();
        assertThat(props.getMessageId()).isEqualTo(resultado.eventId());
        assertThat(props.getCorrelationId()).isEqualTo("OT-55");
        assertThat(props.getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_JSON);
    }

    @Test
    void flujoInexistenteEs404SinPublicar() {
        assertThatThrownBy(() -> publicador.publicar("sms", evento(null)))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessageContaining("sms");
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void siElBrokerRechazaElEventoLaPublicacionFalla() {
        brokerResponde(false, false);

        assertThatThrownBy(() -> publicador.publicar("email", evento("CREADA")))
                .isInstanceOf(PublicacionFallidaException.class)
                .hasMessageContaining("rechazó");
    }

    @Test
    void siNingunaColaRecibeElEventoLaPublicacionFalla() {
        brokerResponde(true, true);

        assertThatThrownBy(() -> publicador.publicar("email", evento("CREADA")))
                .isInstanceOf(PublicacionFallidaException.class)
                .hasMessageContaining("Ninguna cola");
    }

    @Test
    void siElBrokerNoEstaDisponibleLaPublicacionFalla() {
        doThrow(new AmqpConnectException(new ConnectException("Connection refused")))
                .when(rabbitTemplate).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));

        assertThatThrownBy(() -> publicador.publicar("email", evento("CREADA")))
                .isInstanceOf(PublicacionFallidaException.class);
    }
}
