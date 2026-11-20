package df.digitalfix_ms_admin_queues.config;

import df.digitalfix_ms_admin_queues.enums.ExchangeType;
import org.springframework.amqp.core.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Declara los beans de la topología centralizada de RabbitMQ a partir de las propiedades
 * definidas en {@code application.yaml} ({@code app.topology}).
 */
@Configuration
@EnableConfigurationProperties(RabbitTopologyProperties.class)
public class RabbitTopologyConfig {

    @Bean
    public Declarables rabbitTopologyDeclarables(RabbitTopologyProperties properties) {
        List<Declarable> declarables = new ArrayList<>();
        Map<String, Exchange> exchangesByName = new HashMap<>();

        for (RabbitTopologyProperties.ExchangeDefinition def : properties.getExchanges()) {
            Exchange exchange = buildExchange(def);
            exchangesByName.put(def.getName(), exchange);
            declarables.add(exchange);
        }

        for (RabbitTopologyProperties.QueueDefinition qDef : properties.getQueues()) {
            QueueBuilder queueBuilder = qDef.isDurable()
                    ? QueueBuilder.durable(qDef.getName())
                    : QueueBuilder.nonDurable(qDef.getName());

            if (hasText(qDef.getDeadLetterExchange())) {
                queueBuilder.deadLetterExchange(qDef.getDeadLetterExchange());
            }
            if (hasText(qDef.getDeadLetterRoutingKey())) {
                queueBuilder.deadLetterRoutingKey(qDef.getDeadLetterRoutingKey());
            }

            Queue mainQueue = queueBuilder.build();
            declarables.add(mainQueue);

            for (RabbitTopologyProperties.BindingDefinition bDef : qDef.getBindings()) {
                Exchange targetExchange = exchangesByName.get(bDef.getExchange());
                if (targetExchange != null) {
                    declarables.add(BindingBuilder.bind(mainQueue).to(targetExchange).with(bDef.getRoutingKey()).noargs());
                }
            }

            if (hasText(qDef.getDlqName())) {
                Queue dlq = qDef.isDurable()
                        ? QueueBuilder.durable(qDef.getDlqName()).build()
                        : QueueBuilder.nonDurable(qDef.getDlqName()).build();
                declarables.add(dlq);

                if (hasText(qDef.getDeadLetterExchange()) && hasText(qDef.getDeadLetterRoutingKey())) {
                    Exchange dlxExchange = exchangesByName.get(qDef.getDeadLetterExchange());
                    if (dlxExchange != null) {
                        declarables.add(BindingBuilder.bind(dlq).to(dlxExchange).with(qDef.getDeadLetterRoutingKey()).noargs());
                    }
                }
            }
        }

        return new Declarables(declarables);
    }

    private Exchange buildExchange(RabbitTopologyProperties.ExchangeDefinition def) {
        String typeUpper = def.getType() != null ? def.getType().toUpperCase() : ExchangeType.DIRECT.name();
        if (ExchangeType.TOPIC.name().equals(typeUpper)) {
            return new TopicExchange(def.getName(), def.isDurable(), def.isAutoDelete());
        } else if (ExchangeType.FANOUT.name().equals(typeUpper)) {
            return new FanoutExchange(def.getName(), def.isDurable(), def.isAutoDelete());
        } else if (ExchangeType.HEADERS.name().equals(typeUpper)) {
            return new HeadersExchange(def.getName(), def.isDurable(), def.isAutoDelete());
        }
        return new DirectExchange(def.getName(), def.isDurable(), def.isAutoDelete());
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
