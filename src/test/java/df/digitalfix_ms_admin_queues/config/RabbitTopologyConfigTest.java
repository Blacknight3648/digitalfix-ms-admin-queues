package df.digitalfix_ms_admin_queues.config;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitTopologyConfigTest {

    private final RabbitTopologyConfig config = new RabbitTopologyConfig();

    static RabbitTopologyProperties sampleProperties() {
        RabbitTopologyProperties props = new RabbitTopologyProperties();

        props.setExchanges(List.of(
                exchange("cmd.direct", "DIRECT"),
                exchange("cmd.topic", "TOPIC"),
                exchange("cmd.dead.dlx", "DIRECT")
        ));

        props.setQueues(List.of(
                queue("q.cmd.email", "q.cmd.email.dlq", "email.send.dlq", "email.send"),
                queue("q.cmd.dispatch", "q.cmd.dispatch.dlq", "dispatch.ticket.dlq", "dispatch.ticket"),
                queue("q.cmd.report", "q.cmd.report.dlq", "report.generate.dlq", "report.generate")
        ));
        return props;
    }

    private static RabbitTopologyProperties.ExchangeDefinition exchange(String name, String type) {
        RabbitTopologyProperties.ExchangeDefinition def = new RabbitTopologyProperties.ExchangeDefinition();
        def.setName(name);
        def.setType(type);
        def.setDurable(true);
        def.setAutoDelete(false);
        return def;
    }

    private static RabbitTopologyProperties.QueueDefinition queue(String name, String dlqName, String dlqRk, String rk) {
        RabbitTopologyProperties.QueueDefinition q = new RabbitTopologyProperties.QueueDefinition();
        q.setName(name);
        q.setDurable(true);
        q.setDeadLetterExchange("cmd.dead.dlx");
        q.setDeadLetterRoutingKey(dlqRk);
        q.setDlqName(dlqName);

        RabbitTopologyProperties.BindingDefinition bDirect = new RabbitTopologyProperties.BindingDefinition();
        bDirect.setExchange("cmd.direct");
        bDirect.setRoutingKey(rk);

        RabbitTopologyProperties.BindingDefinition bTopic = new RabbitTopologyProperties.BindingDefinition();
        bTopic.setExchange("cmd.topic");
        bTopic.setRoutingKey(rk);

        q.setBindings(List.of(bDirect, bTopic));
        return q;
    }

    @Test
    void declaraExchangesColasDlqsYBindingsDesdePropiedades() {
        Declarables declarables = config.rabbitTopologyDeclarables(sampleProperties());

        List<Exchange> exchanges = declarables.getDeclarablesByType(Exchange.class);
        List<Queue> queues = declarables.getDeclarablesByType(Queue.class);
        List<Binding> bindings = declarables.getDeclarablesByType(Binding.class);

        assertThat(exchanges).extracting(Exchange::getName)
                .containsExactly("cmd.direct", "cmd.topic", "cmd.dead.dlx");
        assertThat(exchanges).allMatch(Exchange::isDurable);

        assertThat(queues).extracting(Queue::getName)
                .containsExactly(
                        "q.cmd.email", "q.cmd.email.dlq",
                        "q.cmd.dispatch", "q.cmd.dispatch.dlq",
                        "q.cmd.report", "q.cmd.report.dlq"
                );
        assertThat(queues).allMatch(Queue::isDurable);

        Queue emailQueue = queues.stream().filter(q -> q.getName().equals("q.cmd.email")).findFirst().orElseThrow();
        assertThat(emailQueue.getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", "email.send.dlq");

        assertThat(bindings).hasSize(9);
        assertThat(bindings).anyMatch(b -> b.getExchange().equals("cmd.direct")
                && b.getDestination().equals("q.cmd.report")
                && b.getRoutingKey().equals("report.generate"));
        assertThat(bindings).anyMatch(b -> b.getExchange().equals("cmd.dead.dlx")
                && b.getDestination().equals("q.cmd.report.dlq")
                && b.getRoutingKey().equals("report.generate.dlq"));
    }
}
