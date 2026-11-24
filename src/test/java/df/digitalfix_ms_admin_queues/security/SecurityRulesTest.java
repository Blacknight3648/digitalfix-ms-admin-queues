package df.digitalfix_ms_admin_queues.security;

import df.digitalfix_ms_admin_queues.controller.QueueAdminController;
import df.digitalfix_ms_admin_queues.controller.TopicAdminController;
import df.digitalfix_ms_admin_queues.dto.InitializeResponseDTO;
import df.digitalfix_ms_admin_queues.service.KafkaAdminService;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import df.digitalfix_ms_admin_queues.service.TopologyInitService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = {QueueAdminController.class, TopicAdminController.class})
@Import({SecurityConfig.class, EntraJwtAuthenticationConverter.class, JsonSecurityErrorHandler.class})
@TestPropertySource(properties = {"AZURE_TENANT_ID=test-tenant", "AZURE_CLIENT_ID=test-client"})
class SecurityRulesTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private UserRoleClient userRoleClient;
    @MockitoBean
    private RabbitAdminService rabbitAdminService;
    @MockitoBean
    private TopologyInitService topologyInitService;
    @MockitoBean
    private KafkaAdminService kafkaAdminService;

    @Test
    void sinTokenResponde401() throws Exception {
        mockMvc.perform(get("/api/v1/queues")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/topics")).andExpect(status().isUnauthorized());
        mockMvc.perform(post("/api/v1/queues/initialize")).andExpect(status().isUnauthorized());
    }

    @Test
    void rolNoPrivilegiadoResponde403() throws Exception {
        mockMvc.perform(post("/api/v1/queues/initialize")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE"))))
                .andExpect(status().isForbidden());

        mockMvc.perform(get("/api/v1/queues")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_TECNICO"))))
                .andExpect(status().isForbidden());

        mockMvc.perform(get("/api/v1/topics")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_TECNICO"))))
                .andExpect(status().isForbidden());

        mockMvc.perform(get("/api/v1/queues")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"))))
                .andExpect(status().isForbidden());
    }

    @Test
    void superadminAccedeAInitializeColasYTopicos() throws Exception {
        when(topologyInitService.initializeAll()).thenReturn(
                InitializeResponseDTO.builder()
                        .status("SUCCESS")
                        .message("OK")
                        .rabbitmq(InitializeResponseDTO.RabbitInitResult.builder()
                                .status("OK").created(true)
                                .exchanges(List.of()).queues(List.of()).bindings(List.of())
                                .build())
                        .kafka(InitializeResponseDTO.KafkaInitResult.builder()
                                .status("OK").created(true)
                                .createdTopics(List.of()).existingTopics(List.of()).topics(List.of())
                                .build())
                        .build()
        );

        mockMvc.perform(post("/api/v1/queues/initialize")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_SUPERADMIN"))))
                .andExpect(status().isCreated());

        mockMvc.perform(get("/api/v1/queues")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_SUPERADMIN"))))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/topics")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_SUPERADMIN"))))
                .andExpect(status().isOk());
    }
}
