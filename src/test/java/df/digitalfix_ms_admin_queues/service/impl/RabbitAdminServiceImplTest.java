package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.config.RabbitTopologyProperties;
import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.dto.ExchangeRequestDTO;
import df.digitalfix_ms_admin_queues.dto.QueueInitializeResponseDTO;
import df.digitalfix_ms_admin_queues.dto.QueueRequestDTO;
import df.digitalfix_ms_admin_queues.exception.ProtectedResourceException;
import df.digitalfix_ms_admin_queues.exception.ResourceAlreadyExistsException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// Lógica de administración encapsulada en el servicio: los controladores solo llaman a estos
// métodos de alto nivel. Se prueba contra un RabbitAdmin simulado (sin broker).
class RabbitAdminServiceImplTest {

    private RabbitAdmin rabbitAdmin;
    private RabbitTemplate rabbitTemplate;
    private RabbitManagementClient managementClient;
    private RabbitTopologyProperties topologyProperties;
    private RabbitAdminServiceImpl servicio;

    @BeforeEach
    void setUp() {
        rabbitAdmin = mock(RabbitAdmin.class);
        rabbitTemplate = mock(RabbitTemplate.class);
        managementClient = mock(RabbitManagementClient.class);
        topologyProperties = buildTopologyProperties();
        when(rabbitAdmin.getRabbitTemplate()).thenReturn(rabbitTemplate);
        servicio = new RabbitAdminServiceImpl(rabbitAdmin, managementClient, topologyProperties);
        ReflectionTestUtils.setField(servicio, "protectedResources", List.of("q.cmd.*", "cmd.*"));
    }

    private RabbitTopologyProperties buildTopologyProperties() {
        RabbitTopologyProperties props = new RabbitTopologyProperties();
        props.setExchanges(List.of(
                exDef("cmd.direct", "DIRECT"),
                exDef("cmd.topic", "TOPIC"),
                exDef("cmd.dead.dlx", "DIRECT")
        ));
        props.setQueues(List.of(
                qDef("q.cmd.email", "q.cmd.email.dlq", "email.send.dlq", "email.send"),
                qDef("q.cmd.dispatch", "q.cmd.dispatch.dlq", "dispatch.ticket.dlq", "dispatch.ticket"),
                qDef("q.cmd.report", "q.cmd.report.dlq", "report.generate.dlq", "report.generate")
        ));
        return props;
    }

    private RabbitTopologyProperties.ExchangeDefinition exDef(String name, String type) {
        RabbitTopologyProperties.ExchangeDefinition def = new RabbitTopologyProperties.ExchangeDefinition();
        def.setName(name);
        def.setType(type);
        return def;
    }

    private RabbitTopologyProperties.QueueDefinition qDef(String name, String dlqName, String dlqRk, String rk) {
        RabbitTopologyProperties.QueueDefinition q = new RabbitTopologyProperties.QueueDefinition();
        q.setName(name);
        q.setDeadLetterExchange("cmd.dead.dlx");
        q.setDeadLetterRoutingKey(dlqRk);
        q.setDlqName(dlqName);
        RabbitTopologyProperties.BindingDefinition b1 = new RabbitTopologyProperties.BindingDefinition();
        b1.setExchange("cmd.direct");
        b1.setRoutingKey(rk);
        RabbitTopologyProperties.BindingDefinition b2 = new RabbitTopologyProperties.BindingDefinition();
        b2.setExchange("cmd.topic");
        b2.setRoutingKey(rk);
        q.setBindings(List.of(b1, b2));
        return q;
    }

    @SuppressWarnings("unchecked")
    private void exchangeExiste(boolean existe) {
        if (existe) {
            when(rabbitTemplate.execute(any(ChannelCallback.class))).thenReturn(null);
        } else {
            when(rabbitTemplate.execute(any(ChannelCallback.class)))
                    .thenThrow(new AmqpIOException(new IOException("NOT_FOUND - no exchange")));
        }
    }

    @Test
    void creaColaConDlxYTtlComoArgumentos() {
        servicio.createQueue(QueueRequestDTO.builder()
                .queueName("q.demo")
                .deadLetterExchange("cmd.dead.dlx")
                .deadLetterRoutingKey("demo.dead")
                .messageTtl(60_000L)
                .build());

        ArgumentCaptor<Queue> cola = ArgumentCaptor.forClass(Queue.class);
        verify(rabbitAdmin).declareQueue(cola.capture());
        assertThat(cola.getValue().getName()).isEqualTo("q.demo");
        assertThat(cola.getValue().isDurable()).isTrue();
        assertThat(cola.getValue().getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", "demo.dead")
                .containsEntry("x-message-ttl", 60_000L);
    }

    @Test
    void crearUnaColaQueYaExisteEsConflicto() {
        when(rabbitAdmin.getQueueProperties("q.demo")).thenReturn(new Properties());

        assertThatThrownBy(() -> servicio.createQueue(QueueRequestDTO.builder().queueName("q.demo").build()))
                .isInstanceOf(ResourceAlreadyExistsException.class);
        verify(rabbitAdmin, never()).declareQueue(any());
    }

    @Test
    void lasColasDelSistemaNoSePuedenBorrar() {
        assertThatThrownBy(() -> servicio.deleteQueue("q.cmd.email", false, false))
                .isInstanceOf(ProtectedResourceException.class);
        verify(rabbitAdmin, never()).deleteQueue(anyString(), anyBoolean(), anyBoolean());
    }

    @Test
    void borrarUnaColaInexistenteEs404() {
        when(rabbitAdmin.getQueueProperties("q.fantasma")).thenReturn(null);

        assertThatThrownBy(() -> servicio.deleteQueue("q.fantasma", false, false))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void creaExchangeTopicSiNoExiste() {
        exchangeExiste(false);

        servicio.createExchange(ExchangeRequestDTO.builder().exchangeName("demo.topic").type("TOPIC").build());

        ArgumentCaptor<Exchange> exchange = ArgumentCaptor.forClass(Exchange.class);
        verify(rabbitAdmin).declareExchange(exchange.capture());
        assertThat(exchange.getValue().getName()).isEqualTo("demo.topic");
        assertThat(exchange.getValue().getType()).isEqualTo(ExchangeTypes.TOPIC);
    }

    @Test
    void crearUnExchangeQueYaExisteEsConflicto() {
        exchangeExiste(true);

        assertThatThrownBy(() -> servicio.createExchange(ExchangeRequestDTO.builder().exchangeName("demo.topic").type("topic").build()))
                .isInstanceOf(ResourceAlreadyExistsException.class);
        verify(rabbitAdmin, never()).declareExchange(any());
    }

    @Test
    void borrarUnExchangeInexistenteEs404() {
        exchangeExiste(false);

        assertThatThrownBy(() -> servicio.deleteExchange("demo.topic"))
                .isInstanceOf(ResourceNotFoundException.class);
        verify(rabbitAdmin, never()).deleteExchange(anyString());
    }

    @Test
    void losExchangesDelSistemaNoSePuedenBorrar() {
        assertThatThrownBy(() -> servicio.deleteExchange("cmd.direct"))
                .isInstanceOf(ProtectedResourceException.class);
    }

    @Test
    void declaraBindingDeColaAExchangeConSuRoutingKey() {
        servicio.declareBinding(new BindingRequestDTO("q.demo", "cmd.topic", "demo.#"));

        ArgumentCaptor<Binding> binding = ArgumentCaptor.forClass(Binding.class);
        verify(rabbitAdmin).declareBinding(binding.capture());
        assertThat(binding.getValue().getDestination()).isEqualTo("q.demo");
        assertThat(binding.getValue().getExchange()).isEqualTo("cmd.topic");
        assertThat(binding.getValue().getRoutingKey()).isEqualTo("demo.#");
    }

    @Test
    void noSeDesvinculaUnaColaDelSistema() {
        assertThatThrownBy(() -> servicio.removeBinding(new BindingRequestDTO("q.cmd.email", "cmd.direct", "email.send")))
                .isInstanceOf(ProtectedResourceException.class);
        verify(rabbitAdmin, never()).removeBinding(any());
    }

    @Test
    void inicializaTopologiaCuandoFaltanRecursos() {
        exchangeExiste(false);
        when(rabbitAdmin.getQueueProperties(anyString())).thenReturn(null);

        QueueInitializeResponseDTO respuesta = servicio.initializeTopology();

        verify(rabbitAdmin).initialize();
        assertThat(respuesta.isCreated()).isTrue();
        assertThat(respuesta.getExchanges()).containsExactly("cmd.direct", "cmd.topic", "cmd.dead.dlx");
        assertThat(respuesta.getQueues()).containsExactlyInAnyOrder(
                "q.cmd.email", "q.cmd.dispatch", "q.cmd.report",
                "q.cmd.email.dlq", "q.cmd.dispatch.dlq", "q.cmd.report.dlq");
        assertThat(respuesta.getBindings()).hasSize(9);
    }

    @Test
    void inicializaTopologiaDeFormaIdempotenteCuandoYaExiste() {
        exchangeExiste(true);
        when(rabbitAdmin.getQueueProperties(anyString())).thenReturn(new Properties());

        QueueInitializeResponseDTO respuesta = servicio.initializeTopology();

        verify(rabbitAdmin).initialize();
        assertThat(respuesta.isCreated()).isFalse();
    }

    @Test
    void delegaConsultaDeColasExchangesYBindingsAlManagementClient() {
        when(managementClient.listQueues()).thenReturn(List.of(java.util.Map.of("name", "q.cmd.email")));
        when(managementClient.listExchanges()).thenReturn(List.of(java.util.Map.of("name", "cmd.direct")));
        when(managementClient.listBindings()).thenReturn(List.of(java.util.Map.of("routing_key", "email.send")));

        assertThat(servicio.listQueues()).hasSize(1);
        assertThat(servicio.listExchanges()).hasSize(1);
        assertThat(servicio.listBindings()).hasSize(1);
        verify(managementClient).listQueues();
        verify(managementClient).listExchanges();
        verify(managementClient).listBindings();
    }
}
