package df.digitalfix_ms_admin_queues.messaging;

import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.stereotype.Component;

/**
 * Registra un listener por cada flujo de {@code app.rabbitmq.flujos} (listener-email,
 * listener-dispatch, listener-report) más el de cuarentena de las DLQ (listener-dlq). Se registran
 * en el {@code RabbitListenerEndpointRegistry} de este proceso, por eso la API de listeners los
 * controla localmente.
 */
@Component
@RequiredArgsConstructor
public class ListenersEntregaConfigurer implements RabbitListenerConfigurer {

    private final TopologiaRabbitProperties topologia;
    private final ClienteEntregaHttp cliente;
    private final ManejadorFallosEntrega manejadorFallos;

    @Override
    public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
        topologia.flujos().forEach((nombre, flujo) -> {
            SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
            endpoint.setId(ListenerIds.deFlujo(nombre));
            endpoint.setQueueNames(flujo.cola());
            endpoint.setAckMode(AcknowledgeMode.MANUAL);
            endpoint.setMessageListener(new ReenviadorMensajes(nombre, flujo, cliente, manejadorFallos));
            registrar.registerEndpoint(endpoint);
        });

        SimpleRabbitListenerEndpoint dlq = new SimpleRabbitListenerEndpoint();
        dlq.setId(ListenerIds.DLQ);
        dlq.setQueueNames(topologia.flujos().values().stream()
                .map(TopologiaRabbitProperties.Flujo::dlq)
                .toArray(String[]::new));
        dlq.setAckMode(AcknowledgeMode.MANUAL);
        dlq.setMessageListener(new AuditorDlq());
        registrar.registerEndpoint(dlq);
    }
}
