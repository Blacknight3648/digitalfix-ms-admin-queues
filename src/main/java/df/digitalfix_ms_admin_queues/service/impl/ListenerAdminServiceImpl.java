package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.dto.ListenerContainerDTO;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import df.digitalfix_ms_admin_queues.service.ListenerAdminService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ListenerAdminServiceImpl implements ListenerAdminService {

    private final RabbitListenerEndpointRegistry registry;
    private final Set<String> pausedListeners = ConcurrentHashMap.newKeySet();

    @Override
    public List<ListenerContainerDTO> getAllListeners() {
        log.info("Consultando lista de todos los Listener Containers registrados");
        return registry.getListenerContainerIds().stream()
                .map(id -> mapToDTO(id, registry.getListenerContainer(id)))
                .collect(Collectors.toList());
    }

    @Override
    public ListenerContainerDTO getListenerById(String listenerId) {
        log.info("Consultando estado del listener: {}", listenerId);
        MessageListenerContainer container = findContainerOrThrow(listenerId);
        return mapToDTO(listenerId, container);
    }

    @Override
    public void startListener(String listenerId) {
        log.info("Iniciando el listener container: {}", listenerId);
        MessageListenerContainer container = findContainerOrThrow(listenerId);
        pausedListeners.remove(listenerId);
        if (!container.isRunning()) {
            container.start();
            log.info("Listener {} iniciado exitosamente", listenerId);
        } else {
            log.info("El listener {} ya se encontraba en ejecución", listenerId);
        }
    }

    @Override
    public void stopListener(String listenerId) {
        log.info("Deteniendo el listener container: {}", listenerId);
        MessageListenerContainer container = findContainerOrThrow(listenerId);
        pausedListeners.remove(listenerId);
        if (container.isRunning()) {
            container.stop();
            log.info("Listener {} detenido exitosamente", listenerId);
        } else {
            log.info("El listener {} ya se encontraba detenido", listenerId);
        }
    }

    @Override
    public void pauseListener(String listenerId) {
        log.info("Pausando el consumo del listener container: {}", listenerId);
        MessageListenerContainer container = findContainerOrThrow(listenerId);
        if (container.isRunning()) {
            container.stop();
        }
        pausedListeners.add(listenerId);
        log.info("Consumo de listener {} pausado exitosamente", listenerId);
    }

    @Override
    public void resumeListener(String listenerId) {
        log.info("Reanudando el consumo del listener container: {}", listenerId);
        MessageListenerContainer container = findContainerOrThrow(listenerId);
        pausedListeners.remove(listenerId);
        if (!container.isRunning()) {
            container.start();
        }
        log.info("Consumo de listener {} reanudado exitosamente", listenerId);
    }

    private MessageListenerContainer findContainerOrThrow(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        if (container == null) {
            throw new ResourceNotFoundException("No se encontró ningún listener container con el ID: " + listenerId);
        }
        return container;
    }

    private ListenerContainerDTO mapToDTO(String listenerId, MessageListenerContainer container) {
        String queues = "";
        if (container instanceof AbstractMessageListenerContainer abstractContainer) {
            queues = Arrays.toString(abstractContainer.getQueueNames());
        }

        return ListenerContainerDTO.builder()
                .listenerId(listenerId)
                .running(container != null && container.isRunning())
                .paused(pausedListeners.contains(listenerId))
                .autoStartup(container != null && container.isAutoStartup())
                .queueNames(queues)
                .build();
    }
}
