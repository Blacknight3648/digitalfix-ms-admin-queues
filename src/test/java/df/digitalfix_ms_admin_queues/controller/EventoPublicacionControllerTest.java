package df.digitalfix_ms_admin_queues.controller;

import df.digitalfix_ms_admin_queues.dto.EventoPublicadoDTO;
import df.digitalfix_ms_admin_queues.exception.PublicacionFallidaException;
import df.digitalfix_ms_admin_queues.service.PublicadorEventosService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Contrato de la API interna de publicación que usan los productores (ms-workorders).
@WebMvcTest(controllers = EventoPublicacionController.class)
class EventoPublicacionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PublicadorEventosService publicador;

    private ResultActions publicar(String flujo, String json) throws Exception {
        return mockMvc.perform(post("/internal/v1/eventos/{flujo}", flujo)
                .contentType(MediaType.APPLICATION_JSON).content(json));
    }

    @Test
    void eventoValidoSeAceptaCon202() throws Exception {
        when(publicador.publicar(eq("email"), any()))
                .thenReturn(new EventoPublicadoDTO("evt-1", "cmd.topic", "email.creada"));

        publicar("email", "{\"eventType\":\"WORK_ORDER_STATUS_CHANGED\",\"correlationId\":\"OT-1\",\"subtipo\":\"CREADA\",\"data\":{\"workOrderId\":1}}")
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value("evt-1"))
                .andExpect(jsonPath("$.routingKey").value("email.creada"));
    }

    @Test
    void sinDataEs400() throws Exception {
        publicar("email", "{\"eventType\":\"WORK_ORDER_STATUS_CHANGED\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.data").exists());
        verifyNoInteractions(publicador);
    }

    @Test
    void dataQueNoEsObjetoEs400() throws Exception {
        publicar("email", "{\"eventType\":\"X\",\"data\":[1,2]}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.dataObjeto").exists());
        verifyNoInteractions(publicador);
    }

    @Test
    void subtipoConPuntosEs400() throws Exception {
        // email.* calza con una sola palabra: "a.b" no llegaría a ninguna cola
        publicar("email", "{\"eventType\":\"X\",\"subtipo\":\"a.b\",\"data\":{}}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.subtipo").exists());
        verifyNoInteractions(publicador);
    }

    @Test
    void brokerNoDisponibleEs503() throws Exception {
        when(publicador.publicar(any(), any())).thenThrow(new PublicacionFallidaException("El broker de mensajería no está disponible"));

        publicar("email", "{\"eventType\":\"X\",\"data\":{}}")
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value("El broker de mensajería no está disponible"));
    }
}
