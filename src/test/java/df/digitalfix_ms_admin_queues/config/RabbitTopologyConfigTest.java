package df.digitalfix_ms_admin_queues.config;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitTopologyConfigTest {

    private final RabbitTopologyConfig config = new RabbitTopologyConfig();

    @Test
    void declaraExchangesDurablesEsperados() {
        DirectExchange direct = config.cmdDirectExchange();
        TopicExchange topic = config.cmdTopicExchange();
        DirectExchange dlx = config.cmdDeadDlxExchange();

        assertThat(direct.getName()).isEqualTo("cmd.direct");
        assertThat(direct.isDurable()).isTrue();
        assertThat(direct.isAutoDelete()).isFalse();

        assertThat(topic.getName()).isEqualTo("cmd.topic");
        assertThat(topic.isDurable()).isTrue();
        assertThat(topic.isAutoDelete()).isFalse();

        assertThat(dlx.getName()).isEqualTo("cmd.dead.dlx");
        assertThat(dlx.isDurable()).isTrue();
        assertThat(dlx.isAutoDelete()).isFalse();
    }

    @Test
    void declaraColasConDlxYDlqsCorrespondientes() {
        Queue emailQueue = config.cmdEmailQueue();
        Queue dispatchQueue = config.cmdDispatchQueue();
        Queue reportQueue = config.cmdReportQueue();
        Queue emailDlq = config.cmdEmailDlq();
        Queue dispatchDlq = config.cmdDispatchDlq();
        Queue reportDlq = config.cmdReportDlq();

        assertThat(emailQueue.getName()).isEqualTo("q.cmd.email");
        assertThat(emailQueue.isDurable()).isTrue();
        assertThat(emailQueue.getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", "email.send.dlq");

        assertThat(dispatchQueue.getName()).isEqualTo("q.cmd.dispatch");
        assertThat(dispatchQueue.isDurable()).isTrue();
        assertThat(dispatchQueue.getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", "dispatch.ticket.dlq");

        assertThat(reportQueue.getName()).isEqualTo("q.cmd.report");
        assertThat(reportQueue.isDurable()).isTrue();
        assertThat(reportQueue.getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", "report.gen.dlq");

        assertThat(emailDlq.getName()).isEqualTo("q.cmd.email.dlq");
        assertThat(emailDlq.isDurable()).isTrue();

        assertThat(dispatchDlq.getName()).isEqualTo("q.cmd.dispatch.dlq");
        assertThat(dispatchDlq.isDurable()).isTrue();

        assertThat(reportDlq.getName()).isEqualTo("q.cmd.report.dlq");
        assertThat(reportDlq.isDurable()).isTrue();
    }

    @Test
    void declaraBindingsConRoutingKeysEsperadas() {
        DirectExchange direct = config.cmdDirectExchange();
        TopicExchange topic = config.cmdTopicExchange();
        DirectExchange dlx = config.cmdDeadDlxExchange();

        Queue emailQueue = config.cmdEmailQueue();
        Queue dispatchQueue = config.cmdDispatchQueue();
        Queue reportQueue = config.cmdReportQueue();
        Queue emailDlq = config.cmdEmailDlq();
        Queue dispatchDlq = config.cmdDispatchDlq();
        Queue reportDlq = config.cmdReportDlq();

        Binding b1 = config.bindEmailToDirect(emailQueue, direct);
        Binding b2 = config.bindEmailToTopic(emailQueue, topic);
        Binding b3 = config.bindDispatchToDirect(dispatchQueue, direct);
        Binding b4 = config.bindDispatchToTopic(dispatchQueue, topic);
        Binding b5 = config.bindEmailDlqToDlx(emailDlq, dlx);
        Binding b6 = config.bindDispatchDlqToDlx(dispatchDlq, dlx);
        Binding b7 = config.bindReportToDirect(reportQueue, direct);
        Binding b8 = config.bindReportToTopic(reportQueue, topic);
        Binding b9 = config.bindReportDlqToDlx(reportDlq, dlx);

        assertThat(b1.getExchange()).isEqualTo("cmd.direct");
        assertThat(b1.getDestination()).isEqualTo("q.cmd.email");
        assertThat(b1.getRoutingKey()).isEqualTo("email.send");

        assertThat(b2.getExchange()).isEqualTo("cmd.topic");
        assertThat(b2.getDestination()).isEqualTo("q.cmd.email");
        assertThat(b2.getRoutingKey()).isEqualTo("email.*");

        assertThat(b3.getExchange()).isEqualTo("cmd.direct");
        assertThat(b3.getDestination()).isEqualTo("q.cmd.dispatch");
        assertThat(b3.getRoutingKey()).isEqualTo("dispatch.ticket");

        assertThat(b4.getExchange()).isEqualTo("cmd.topic");
        assertThat(b4.getDestination()).isEqualTo("q.cmd.dispatch");
        assertThat(b4.getRoutingKey()).isEqualTo("dispatch.#");

        assertThat(b5.getExchange()).isEqualTo("cmd.dead.dlx");
        assertThat(b5.getDestination()).isEqualTo("q.cmd.email.dlq");
        assertThat(b5.getRoutingKey()).isEqualTo("email.send.dlq");

        assertThat(b6.getExchange()).isEqualTo("cmd.dead.dlx");
        assertThat(b6.getDestination()).isEqualTo("q.cmd.dispatch.dlq");
        assertThat(b6.getRoutingKey()).isEqualTo("dispatch.ticket.dlq");

        assertThat(b7.getExchange()).isEqualTo("cmd.direct");
        assertThat(b7.getDestination()).isEqualTo("q.cmd.report");
        assertThat(b7.getRoutingKey()).isEqualTo("report.gen");

        assertThat(b8.getExchange()).isEqualTo("cmd.topic");
        assertThat(b8.getDestination()).isEqualTo("q.cmd.report");
        assertThat(b8.getRoutingKey()).isEqualTo("report.*");

        assertThat(b9.getExchange()).isEqualTo("cmd.dead.dlx");
        assertThat(b9.getDestination()).isEqualTo("q.cmd.report.dlq");
        assertThat(b9.getRoutingKey()).isEqualTo("report.gen.dlq");
    }
}
