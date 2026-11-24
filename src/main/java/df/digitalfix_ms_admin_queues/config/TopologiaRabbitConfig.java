package df.digitalfix_ms_admin_queues.config;

import df.digitalfix_ms_admin_queues.config.TopologiaRabbitProperties.Flujo;
import org.springframework.amqp.core.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Topología RabbitMQ del caso DigitalFix (sección 8), declarada por ms-admin-queues como dueño
 * único de la mensajería: 3 exchanges y, por cada flujo de {@code app.rabbitmq.flujos}, su cola de
 * trabajo, su DLQ y 3 bindings (direct, topic y DLQ). Al conectarse, RabbitAdmin los crea en el
 * broker si no existen (la declaración es idempotente).
 * <p>
 * Todas las colas son quorum: se replican en los 2 nodos del clúster, así un mensaje confirmado no
 * se pierde si cae un nodo.
 *
 * <pre>
 *   productor ─HTTP─► ms-admin-queues ──► cmd.direct ──email.send──► q.cmd.email ──NACK──► cmd.dead.dlx ──email.dead──► q.cmd.email.dlq
 *                                    └──► cmd.topic  ──email.*────┘      │
 *                                                                        └─ listener-email ─HTTP─► ms-notify /internal/v1/notificaciones/email
 * </pre>
 * (igual para dispatch y report).
 */
@Configuration
@EnableConfigurationProperties(TopologiaRabbitProperties.class)
public class TopologiaRabbitConfig {

    private final TopologiaRabbitProperties topologia;

    public TopologiaRabbitConfig(TopologiaRabbitProperties topologia) {
        this.topologia = topologia;
    }

    /** Todos los recursos del sistema en un solo bean: RabbitAdmin los declara al conectar. */
    @Bean
    public Declarables topologiaDigitalFix() {
        DirectExchange direct = ExchangeBuilder.directExchange(topologia.exchanges().direct()).durable(true).build();
        TopicExchange topic = ExchangeBuilder.topicExchange(topologia.exchanges().topic()).durable(true).build();
        DirectExchange dlx = ExchangeBuilder.directExchange(topologia.exchanges().dlx()).durable(true).build();

        List<Declarable> recursos = new ArrayList<>(List.of(direct, topic, dlx));
        topologia.flujos().values().forEach(flujo -> {
            Queue cola = colaPrincipal(flujo);
            Queue dlq = colaMuerta(flujo);
            recursos.add(cola);
            recursos.add(dlq);
            recursos.add(BindingBuilder.bind(cola).to(direct).with(flujo.routingKey()));
            recursos.add(BindingBuilder.bind(cola).to(topic).with(flujo.patronTopic()));
            recursos.add(BindingBuilder.bind(dlq).to(dlx).with(flujo.routingKeyDlq()));
        });
        return new Declarables(recursos);
    }

    /** Cola de trabajo: lo que se rechaza con NACK sin requeue va al DLX con la routing key de su DLQ. */
    Queue colaPrincipal(Flujo flujo) {
        return QueueBuilder.durable(flujo.cola())
                .quorum()
                .deadLetterExchange(topologia.exchanges().dlx())
                .deadLetterRoutingKey(flujo.routingKeyDlq())
                .build();
    }

    /** DLQ: guarda los mensajes fallidos para revisarlos (el listener-dlq los registra en el log). */
    Queue colaMuerta(Flujo flujo) {
        return QueueBuilder.durable(flujo.dlq())
                .quorum()
                .ttl((int) topologia.dlqTtl().toMillis())
                .build();
    }
}
