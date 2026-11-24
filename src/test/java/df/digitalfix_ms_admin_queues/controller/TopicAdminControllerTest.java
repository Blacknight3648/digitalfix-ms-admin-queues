package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.TopicDetailDTO;
import df.digitalfix_ms_admin_queues.dto.TopicSummaryDTO;
import df.digitalfix_ms_admin_queues.exception.BrokerUnavailableException;
import df.digitalfix_ms_admin_queues.exception.ProtectedResourceException;
import df.digitalfix_ms_admin_queues.exception.ResourceAlreadyExistsException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import df.digitalfix_ms_admin_queues.security.EntraJwtAuthenticationConverter;
import df.digitalfix_ms_admin_queues.security.JsonSecurityErrorHandler;
import df.digitalfix_ms_admin_queues.security.SecurityConfig;
import df.digitalfix_ms_admin_queues.security.UserRoleClient;
import df.digitalfix_ms_admin_queues.service.KafkaAdminService;
import org.junit.jupiter.api.Test;
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

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = TopicAdminController.class)
@Import({SecurityConfig.class, EntraJwtAuthenticationConverter.class, JsonSecurityErrorHandler.class})
@TestPropertySource(properties = {"AZURE_TENANT_ID=test-tenant", "AZURE_CLIENT_ID=test-client"})
class TopicAdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private KafkaAdminService kafkaAdminService;
    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private UserRoleClient userRoleClient;

    private static RequestPostProcessor adminJwt() {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
    }

    @Test
    void listaTopicosDeNegocioCon200() throws Exception {
        when(kafkaAdminService.listTopics()).thenReturn(List.of(
                TopicSummaryDTO.builder().name("audit.timeline").partitions(3).replicationFactor(1).internal(false).build(),
                TopicSummaryDTO.builder().name("workorders.events").partitions(3).replicationFactor(1).internal(false).build()
        ));

        mockMvc.perform(get("/api/v1/topics").with(adminJwt()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].name").value("audit.timeline"))
                .andExpect(jsonPath("$[1].name").value("workorders.events"));
    }

    @Test
    void consultaDetalleDeTopicoCon200Y404SiNoExiste() throws Exception {
        when(kafkaAdminService.getTopicDetails("workorders.events")).thenReturn(
                TopicDetailDTO.builder()
                        .name("workorders.events")
                        .partitions(3)
                        .replicationFactor(1)
                        .internal(false)
                        .partitionDetails(List.of(TopicDetailDTO.PartitionInfoDTO.builder()
                                .partition(0).leader(1).replicas(List.of(1)).isr(List.of(1)).build()))
                        .configs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"))
                        .build()
        );
        when(kafkaAdminService.getTopicDetails("no.existe"))
                .thenThrow(new ResourceNotFoundException("El tópico especificado no existe en Kafka"));

        mockMvc.perform(get("/api/v1/topics/workorders.events").with(adminJwt()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitions").value(3))
                .andExpect(jsonPath("$.configs['retention.ms']").value("604800000"));

        mockMvc.perform(get("/api/v1/topics/no.existe").with(adminJwt()))
                .andExpect(status().isNotFound());
    }

    @Test
    void creaTopicoValidoCon201YDuplicadoCon409() throws Exception {
        when(kafkaAdminService.createTopic(any())).thenReturn(
                TopicSummaryDTO.builder().name("custom.events").partitions(3).replicationFactor(1).internal(false).build()
        );

        mockMvc.perform(post("/api/v1/topics").with(adminJwt())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"custom.events\",\"partitions\":3,\"replicationFactor\":1}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("custom.events"))
                .andExpect(jsonPath("$.partitions").value(3));

        when(kafkaAdminService.createTopic(any()))
                .thenThrow(new ResourceAlreadyExistsException("El tópico ya existe"));

        mockMvc.perform(post("/api/v1/topics").with(adminJwt())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"custom.events\",\"partitions\":3}"))
                .andExpect(status().isConflict());
    }

    @Test
    void rechazaParticionesMenoresATresONombreInternoCon400() throws Exception {
        mockMvc.perform(post("/api/v1/topics").with(adminJwt())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"custom.events\",\"partitions\":2}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.partitions").exists());

        mockMvc.perform(post("/api/v1/topics").with(adminJwt())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"__consumer_offsets\",\"partitions\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.nombrePermitido").exists());

        verifyNoInteractions(kafkaAdminService);
    }

    @Test
    void borrarTopicoProtegidoResponde403YNoProtegidoResponde200() throws Exception {
        doThrow(new ProtectedResourceException("'workorders.events' es un recurso protegido"))
                .when(kafkaAdminService).deleteTopic("workorders.events");

        mockMvc.perform(delete("/api/v1/topics/workorders.events").with(adminJwt()))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403));

        mockMvc.perform(delete("/api/v1/topics/custom.events").with(adminJwt()))
                .andExpect(status().isOk());
        verify(kafkaAdminService).deleteTopic("custom.events");
    }

    @Test
    void brokerCaidoResponde503() throws Exception {
        when(kafkaAdminService.listTopics())
                .thenThrow(new BrokerUnavailableException("El clúster de Kafka no responde", new RuntimeException("timeout")));

        mockMvc.perform(get("/api/v1/topics").with(adminJwt()))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));
    }
}
