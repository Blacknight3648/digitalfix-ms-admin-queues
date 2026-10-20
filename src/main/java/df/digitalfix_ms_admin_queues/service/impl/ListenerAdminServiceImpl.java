package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.dto.ListenerContainerDTO;
import df.digitalfix_ms_admin_queues.exception.AdminQueueException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import df.digitalfix_ms_admin_queues.service.ListenerAdminService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;

/**
 * Control remoto de los listeners de RabbitMQ.
 *
 * <p>Los {@code @RabbitListener} viven en el microservicio que consume las colas (ms-notify), y el
 * {@code RabbitListenerEndpointRegistry} solo ve los contenedores de su propio proceso. Por eso este
 * administrador centralizado no los consulta localmente: delega por HTTP en
 * {@code /api/v1/listeners} de ms-notify, por la red interna digitalfix-net.
 */
@Slf4j
@Service
public class ListenerAdminServiceImpl implements ListenerAdminService {

    private static final String RUTA = "/api/v1/listeners";

    private final RestClient notify;

    public ListenerAdminServiceImpl(@Value("${app.listeners.notify-url}") String notifyUrl) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(10000);
        this.notify = RestClient.builder().baseUrl(notifyUrl).requestFactory(factory).build();
    }

    @Override
    public List<ListenerContainerDTO> getAllListeners() {
        log.info("Consultando los listeners registrados en ms-notify");
        return llamar(() -> notify.get().uri(RUTA).retrieve()
                .body(new ParameterizedTypeReference<List<ListenerContainerDTO>>() { }), null);
    }

    @Override
    public ListenerContainerDTO getListenerById(String listenerId) {
        log.info("Consultando estado del listener: {}", listenerId);
        return llamar(() -> notify.get().uri(RUTA + "/{id}", listenerId).retrieve()
                .body(ListenerContainerDTO.class), listenerId);
    }

    @Override
    public void startListener(String listenerId) {
        accion(listenerId, "start");
    }

    @Override
    public void stopListener(String listenerId) {
        accion(listenerId, "stop");
    }

    @Override
    public void pauseListener(String listenerId) {
        accion(listenerId, "pause");
    }

    @Override
    public void resumeListener(String listenerId) {
        accion(listenerId, "resume");
    }

    private void accion(String listenerId, String accion) {
        log.info("Solicitando '{}' del listener {} a ms-notify", accion, listenerId);
        llamar(() -> notify.post().uri(RUTA + "/{id}/{accion}", listenerId, accion).retrieve().toBodilessEntity(),
                listenerId);
    }

    private <T> T llamar(java.util.function.Supplier<T> llamada, String listenerId) {
        try {
            return llamada.get();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new ResourceNotFoundException("No se encontró ningún listener container con el ID: " + listenerId);
            }
            throw new AdminQueueException("ms-notify rechazó la operación sobre el listener: " + e.getStatusText(), e);
        } catch (RestClientException e) {
            log.error("ms-notify no responde: {}", e.getMessage());
            throw new AdminQueueException("No se pudo contactar al servicio dueño de los listeners (ms-notify)", e);
        }
    }
}
