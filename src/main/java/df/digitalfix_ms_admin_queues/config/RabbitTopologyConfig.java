package df.digitalfix_ms_admin_queues.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declara los beans de la topología base de RabbitMQ para mensajería de comandos:
 * exchanges (cmd.direct, cmd.topic, cmd.dead.dlx), colas principales con su DLX/DLQ
 * y los bindings correspondientes.
 */
@Configuration
public class RabbitTopologyConfig {

    public static final String EXCHANGE_CMD_DIRECT = "cmd.direct";
    public static final String EXCHANGE_CMD_TOPIC = "cmd.topic";
    public static final String EXCHANGE_CMD_DEAD_DLX = "cmd.dead.dlx";

    public static final String QUEUE_CMD_EMAIL = "q.cmd.email";
    public static final String QUEUE_CMD_DISPATCH = "q.cmd.dispatch";
    public static final String QUEUE_CMD_REPORT = "q.cmd.report";
    public static final String QUEUE_CMD_EMAIL_DLQ = "q.cmd.email.dlq";
    public static final String QUEUE_CMD_DISPATCH_DLQ = "q.cmd.dispatch.dlq";
    public static final String QUEUE_CMD_REPORT_DLQ = "q.cmd.report.dlq";

    public static final String ROUTING_KEY_EMAIL_SEND = "email.send";
    public static final String ROUTING_KEY_DISPATCH_TICKET = "dispatch.ticket";
    public static final String ROUTING_KEY_REPORT_GEN = "report.gen";
    public static final String ROUTING_KEY_TOPIC_EMAIL = "email.*";
    public static final String ROUTING_KEY_TOPIC_DISPATCH = "dispatch.#";
    public static final String ROUTING_KEY_TOPIC_REPORT = "report.*";
    public static final String ROUTING_KEY_EMAIL_DLQ = "email.send.dlq";
    public static final String ROUTING_KEY_DISPATCH_DLQ = "dispatch.ticket.dlq";
    public static final String ROUTING_KEY_REPORT_DLQ = "report.gen.dlq";

    @Bean
    public DirectExchange cmdDirectExchange() {
        return new DirectExchange(EXCHANGE_CMD_DIRECT, true, false);
    }

    @Bean
    public TopicExchange cmdTopicExchange() {
        return new TopicExchange(EXCHANGE_CMD_TOPIC, true, false);
    }

    @Bean
    public DirectExchange cmdDeadDlxExchange() {
        return new DirectExchange(EXCHANGE_CMD_DEAD_DLX, true, false);
    }

    @Bean
    public Queue cmdEmailQueue() {
        return QueueBuilder.durable(QUEUE_CMD_EMAIL)
                .deadLetterExchange(EXCHANGE_CMD_DEAD_DLX)
                .deadLetterRoutingKey(ROUTING_KEY_EMAIL_DLQ)
                .build();
    }

    @Bean
    public Queue cmdDispatchQueue() {
        return QueueBuilder.durable(QUEUE_CMD_DISPATCH)
                .deadLetterExchange(EXCHANGE_CMD_DEAD_DLX)
                .deadLetterRoutingKey(ROUTING_KEY_DISPATCH_DLQ)
                .build();
    }

    @Bean
    public Queue cmdReportQueue() {
        return QueueBuilder.durable(QUEUE_CMD_REPORT)
                .deadLetterExchange(EXCHANGE_CMD_DEAD_DLX)
                .deadLetterRoutingKey(ROUTING_KEY_REPORT_DLQ)
                .build();
    }

    @Bean
    public Queue cmdEmailDlq() {
        return QueueBuilder.durable(QUEUE_CMD_EMAIL_DLQ).build();
    }

    @Bean
    public Queue cmdDispatchDlq() {
        return QueueBuilder.durable(QUEUE_CMD_DISPATCH_DLQ).build();
    }

    @Bean
    public Queue cmdReportDlq() {
        return QueueBuilder.durable(QUEUE_CMD_REPORT_DLQ).build();
    }

    @Bean
    public Binding bindEmailToDirect(Queue cmdEmailQueue, DirectExchange cmdDirectExchange) {
        return BindingBuilder.bind(cmdEmailQueue).to(cmdDirectExchange).with(ROUTING_KEY_EMAIL_SEND);
    }

    @Bean
    public Binding bindEmailToTopic(Queue cmdEmailQueue, TopicExchange cmdTopicExchange) {
        return BindingBuilder.bind(cmdEmailQueue).to(cmdTopicExchange).with(ROUTING_KEY_TOPIC_EMAIL);
    }

    @Bean
    public Binding bindDispatchToDirect(Queue cmdDispatchQueue, DirectExchange cmdDirectExchange) {
        return BindingBuilder.bind(cmdDispatchQueue).to(cmdDirectExchange).with(ROUTING_KEY_DISPATCH_TICKET);
    }

    @Bean
    public Binding bindDispatchToTopic(Queue cmdDispatchQueue, TopicExchange cmdTopicExchange) {
        return BindingBuilder.bind(cmdDispatchQueue).to(cmdTopicExchange).with(ROUTING_KEY_TOPIC_DISPATCH);
    }

    @Bean
    public Binding bindEmailDlqToDlx(Queue cmdEmailDlq, DirectExchange cmdDeadDlxExchange) {
        return BindingBuilder.bind(cmdEmailDlq).to(cmdDeadDlxExchange).with(ROUTING_KEY_EMAIL_DLQ);
    }

    @Bean
    public Binding bindDispatchDlqToDlx(Queue cmdDispatchDlq, DirectExchange cmdDeadDlxExchange) {
        return BindingBuilder.bind(cmdDispatchDlq).to(cmdDeadDlxExchange).with(ROUTING_KEY_DISPATCH_DLQ);
    }

    @Bean
    public Binding bindReportToDirect(Queue cmdReportQueue, DirectExchange cmdDirectExchange) {
        return BindingBuilder.bind(cmdReportQueue).to(cmdDirectExchange).with(ROUTING_KEY_REPORT_GEN);
    }

    @Bean
    public Binding bindReportToTopic(Queue cmdReportQueue, TopicExchange cmdTopicExchange) {
        return BindingBuilder.bind(cmdReportQueue).to(cmdTopicExchange).with(ROUTING_KEY_TOPIC_REPORT);
    }

    @Bean
    public Binding bindReportDlqToDlx(Queue cmdReportDlq, DirectExchange cmdDeadDlxExchange) {
        return BindingBuilder.bind(cmdReportDlq).to(cmdDeadDlxExchange).with(ROUTING_KEY_REPORT_DLQ);
    }
}
