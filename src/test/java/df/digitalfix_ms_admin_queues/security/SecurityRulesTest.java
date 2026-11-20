package df.digitalfix_ms_admin_queues.security;

import df.digitalfix_ms_admin_queues.controller.QueueAdminController;
import df.digitalfix_ms_admin_queues.dto.QueueInitializeResponseDTO;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
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

@WebMvcTest(controllers = QueueAdminController.class)
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

    @Test
    void sinTokenResponde401() throws Exception {
        mockMvc.perform(get("/api/v1/queues")).andExpect(status().isUnauthorized());
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
    }

    @Test
    void adminYSuperadminAccedenAInitializeYColas() throws Exception {
        when(rabbitAdminService.initializeTopology()).thenReturn(
                QueueInitializeResponseDTO.builder()
                        .created(true)
                        .message("OK")
                        .exchanges(List.of())
                        .queues(List.of())
                        .bindings(List.of())
                        .build()
        );

        mockMvc.perform(post("/api/v1/queues/initialize")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"))))
                .andExpect(status().isCreated());

        mockMvc.perform(get("/api/v1/queues")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_SUPERADMIN"))))
                .andExpect(status().isOk());
    }
}
