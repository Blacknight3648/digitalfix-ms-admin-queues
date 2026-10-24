package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.exception.ResourceAlreadyExistsException;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Contrato REST del administrador: rutas, verbos, códigos y validación de entrada. Una petición
// inválida responde 400 con el detalle y nunca llega al servicio (ni a RabbitMQ).
@WebMvcTest(controllers = {QueueAdminController.class, ExchangeAdminController.class, BindingAdminController.class})
class AdminApiValidacionTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RabbitAdminService servicio;

    private org.springframework.test.web.servlet.ResultActions postJson(String ruta, String json) throws Exception {
        return mockMvc.perform(post(ruta).contentType(MediaType.APPLICATION_JSON).content(json));
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
        mockMvc.perform(delete("/api/v1/queues/{name}", "cola con espacios"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(servicio);
    }

    @Test
    void desvincularUsaQueryParamsYAceptaComodines() throws Exception {
        mockMvc.perform(delete("/api/v1/bindings")
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
        mockMvc.perform(delete("/api/v1/bindings").param("exchange", "cmd.topic").param("queue", "q.demo"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Falta el parámetro obligatorio 'routingKey'"));
        verifyNoInteractions(servicio);
    }
}
