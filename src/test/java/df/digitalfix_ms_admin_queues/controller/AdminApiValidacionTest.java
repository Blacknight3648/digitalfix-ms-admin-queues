package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.dto.InitializeResponseDTO;
import df.digitalfix_ms_admin_queues.exception.ProtectedResourceException;
import df.digitalfix_ms_admin_queues.exception.ResourceAlreadyExistsException;
import df.digitalfix_ms_admin_queues.security.EntraJwtAuthenticationConverter;
import df.digitalfix_ms_admin_queues.security.JsonSecurityErrorHandler;
import df.digitalfix_ms_admin_queues.security.SecurityConfig;
import df.digitalfix_ms_admin_queues.security.UserRoleClient;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import df.digitalfix_ms_admin_queues.service.TopologyInitService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Contrato REST del administrador: rutas, verbos, códigos y validación de entrada. Una petición
// inválida responde 400 con el detalle y nunca llega al servicio (ni a RabbitMQ).
@WebMvcTest(controllers = {QueueAdminController.class, ExchangeAdminController.class, BindingAdminController.class})
@Import({SecurityConfig.class, EntraJwtAuthenticationConverter.class, JsonSecurityErrorHandler.class})
@TestPropertySource(properties = {"AZURE_TENANT_ID=test-tenant", "AZURE_CLIENT_ID=test-client"})
class AdminApiValidacionTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RabbitAdminService servicio;
    @MockitoBean
    private TopologyInitService topologyInitService;
    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private UserRoleClient userRoleClient;

    private static RequestPostProcessor adminJwt() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_SUPERADMIN"));
    }

    private org.springframework.test.web.servlet.ResultActions postJson(String ruta, String json) throws Exception {
        return mockMvc.perform(post(ruta).with(adminJwt()).contentType(MediaType.APPLICATION_JSON).content(json));
    }

    @Test
    void creaColaValidaCon201() throws Exception {
        postJson("/api/v1/queues", "{\"queueName\":\"q.demo\",\"deadLetterExchange\":\"cmd.dead.dlx\",\"deadLetterRoutingKey\":\"demo.dead\"}")
                .andExpect(status().isCreated());
        verify(servicio).createQueue(any());
    }

    @Test
    void colaConNombreVacioEs400() throws Exception {
        postJson("/api/v1/queues", "{\"queueName\":\"\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.queueName").exists());
        verifyNoInteractions(servicio);
    }

    @Test
    void colaConCaracteresInvalidosEs400() throws Exception {
        postJson("/api/v1/queues", "{\"queueName\":\"cola con espacios\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.queueName").exists());
    }

    @Test
    void colaExclusivaEs400() throws Exception {
        postJson("/api/v1/queues", "{\"queueName\":\"q.demo\",\"exclusive\":true}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.exclusive").exists());
        verifyNoInteractions(servicio);
    }

    @Test
    void routingKeyDeDlqSinDlxEs400() throws Exception {
        postJson("/api/v1/queues", "{\"queueName\":\"q.demo\",\"deadLetterRoutingKey\":\"demo.dead\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.deadLetterConsistente").exists());
    }

    @Test
    void ttlNegativoEs400() throws Exception {
        postJson("/api/v1/queues", "{\"queueName\":\"q.demo\",\"messageTtl\":-5}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.messageTtl").exists());
    }

    @Test
    void colaDuplicadaEs409() throws Exception {
        doThrow(new ResourceAlreadyExistsException("Ya existe una cola llamada 'q.demo'")).when(servicio).createQueue(any());

        postJson("/api/v1/queues", "{\"queueName\":\"q.demo\"}")
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("Ya existe una cola llamada 'q.demo'"));
    }

    @Test
    void exchangeDeTipoDesconocidoEs400() throws Exception {
        postJson("/api/v1/exchanges", "{\"exchangeName\":\"demo.x\",\"type\":\"broadcast\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.type").exists());
        verifyNoInteractions(servicio);
    }

    @Test
    void nombreInvalidoEnLaRutaEs400() throws Exception {
        mockMvc.perform(delete("/api/v1/queues/{name}", "cola con espacios").with(adminJwt()))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(servicio);
    }

    @Test
    void desvincularUsaQueryParamsYAceptaComodines() throws Exception {
        mockMvc.perform(delete("/api/v1/bindings").with(adminJwt())
                        .param("exchange", "cmd.topic")
                        .param("queue", "q.demo")
                        .param("routingKey", "demo.#"))
                .andExpect(status().isOk());

        ArgumentCaptor<BindingRequestDTO> binding = ArgumentCaptor.forClass(BindingRequestDTO.class);
        verify(servicio).removeBinding(binding.capture());
        assertThat(binding.getValue().getRoutingKey()).isEqualTo("demo.#");
    }

    @Test
    void desvincularSinRoutingKeyEs400() throws Exception {
        mockMvc.perform(delete("/api/v1/bindings").with(adminJwt()).param("exchange", "cmd.topic").param("queue", "q.demo"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Falta el parámetro obligatorio 'routingKey'"));
        verifyNoInteractions(servicio);
    }

    @Test
    void inicializarTopologiaUnificadaDevuelve200EnExito207EnParcialY503SiAmbosFallan() throws Exception {
        when(topologyInitService.initializeAll()).thenReturn(
                InitializeResponseDTO.builder()
                        .status("SUCCESS")
                        .created(true)
                        .message("Topología unificada de RabbitMQ y Kafka asegurada exitosamente.")
                        .rabbitmq(InitializeResponseDTO.RabbitInitResult.builder()
                                .status("OK").created(true)
                                .exchanges(java.util.List.of("cmd.direct", "cmd.topic", "cmd.dead.dlx"))
                                .queues(java.util.List.of("q.cmd.email", "q.cmd.dispatch"))
                                .bindings(java.util.List.of())
                                .build())
                        .kafka(InitializeResponseDTO.KafkaInitResult.builder()
                                .status("OK").created(true)
                                .createdTopics(java.util.List.of("workorders.events", "audit.timeline"))
                                .existingTopics(java.util.List.of())
                                .topics(java.util.List.of("workorders.events", "audit.timeline"))
                                .build())
                        .build(),
                InitializeResponseDTO.builder()
                        .status("SUCCESS")
                        .created(false)
                        .message("Topología unificada de RabbitMQ y Kafka asegurada exitosamente.")
                        .rabbitmq(InitializeResponseDTO.RabbitInitResult.builder().status("OK").created(false).build())
                        .kafka(InitializeResponseDTO.KafkaInitResult.builder().status("OK").created(false).build())
                        .build(),
                InitializeResponseDTO.builder()
                        .status("PARTIAL")
                        .message("Inicialización parcial: uno de los brokers presentó fallos.")
                        .rabbitmq(InitializeResponseDTO.RabbitInitResult.builder().status("OK").build())
                        .kafka(InitializeResponseDTO.KafkaInitResult.builder().status("FAILED").error("Timeout").build())
                        .build(),
                InitializeResponseDTO.builder()
                        .status("FAILED")
                        .message("No se pudo inicializar ni RabbitMQ ni Apache Kafka.")
                        .rabbitmq(InitializeResponseDTO.RabbitInitResult.builder().status("FAILED").build())
                        .kafka(InitializeResponseDTO.KafkaInitResult.builder().status("FAILED").build())
                        .build()
        );

        mockMvc.perform(post("/api/v1/queues/initialize").with(adminJwt()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.created").value(true))
                .andExpect(jsonPath("$.rabbitmq.exchanges.length()").value(3))
                .andExpect(jsonPath("$.kafka.topics.length()").value(2));

        mockMvc.perform(post("/api/v1/queues/initialize").with(adminJwt()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.created").value(false));

        mockMvc.perform(post("/api/v1/queues/initialize").with(adminJwt()))
                .andExpect(status().isMultiStatus())
                .andExpect(jsonPath("$.status").value("PARTIAL"))
                .andExpect(jsonPath("$.kafka.status").value("FAILED"));

        mockMvc.perform(post("/api/v1/queues/initialize").with(adminJwt()))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("FAILED"));

        verify(topologyInitService, times(4)).initializeAll();
    }

    @Test
    void borrarRecursoProtegidoResponde403() throws Exception {
        doThrow(new ProtectedResourceException("'q.cmd.email' está protegido"))
                .when(servicio).deleteQueue("q.cmd.email", false, false);

        mockMvc.perform(delete("/api/v1/queues/q.cmd.email").with(adminJwt()))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403));
    }

    @Test
    void consultaColasExchangesYBindingsCon200() throws Exception {
        when(servicio.listQueues()).thenReturn(java.util.List.of(java.util.Map.of("name", "q.cmd.email")));
        when(servicio.listExchanges()).thenReturn(java.util.List.of(java.util.Map.of("name", "cmd.direct")));
        when(servicio.listBindings()).thenReturn(java.util.List.of(java.util.Map.of("routing_key", "email.send")));

        mockMvc.perform(get("/api/v1/queues").with(adminJwt()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("q.cmd.email"));

        mockMvc.perform(get("/api/v1/exchanges").with(adminJwt()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("cmd.direct"));

        mockMvc.perform(get("/api/v1/bindings").with(adminJwt()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].routing_key").value("email.send"));

        verify(servicio).listQueues();
        verify(servicio).listExchanges();
        verify(servicio).listBindings();
    }
}
