package df.digitalfix_ms_admin_queues.config;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Queue;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

// La topología se arma con el application.yaml real: si alguien cambia un nombre y queda distinto
// al caso (sección 8), este test lo detecta sin levantar RabbitMQ.
class TopologiaRabbitConfigTest {

    private static TopologiaRabbitProperties topologia;
    private static Map<String, Exchange> exchanges;
    private static Map<String, Queue> colas;
    private static Declarables declarables;

    @BeforeAll
    static void cargarYaml() {
        topologia = TopologiaDePrueba.cargar();
        declarables = new TopologiaRabbitConfig(topologia).topologiaDigitalFix();
        exchanges = declarables.getDeclarablesByType(Exchange.class).stream()
                .collect(Collectors.toMap(Exchange::getName, e -> e));
        colas = declarables.getDeclarablesByType(Queue.class).stream()
                .collect(Collectors.toMap(Queue::getName, q -> q));
    }

    @Test
    void declaraLosTresExchangesDelCaso() {
        assertThat(exchanges).hasSize(3);
        assertExchange(exchanges.get("cmd.direct"), ExchangeTypes.DIRECT);
        assertExchange(exchanges.get("cmd.topic"), ExchangeTypes.TOPIC);
        assertExchange(exchanges.get("cmd.dead.dlx"), ExchangeTypes.DIRECT);
    }

    @Test
    void cadaColaDeTrabajoDerivaASuDlqPorElDlx() {
        assertColaConDlq(colas.get("q.cmd.email"), "email.dead");
        assertColaConDlq(colas.get("q.cmd.dispatch"), "dispatch.dead");
        assertColaConDlq(colas.get("q.cmd.report"), "report.dead");
    }

    @Test
    void lasDlqSonQuorumYConservanLosMensajes24Horas() {
        for (String nombre : new String[]{"q.cmd.email.dlq", "q.cmd.dispatch.dlq", "q.cmd.report.dlq"}) {
            Queue dlq = colas.get(nombre);
            assertThat(dlq).as(nombre).isNotNull();
            assertThat(dlq.isDurable()).isTrue();
            assertThat(dlq.getArguments()).containsEntry("x-queue-type", "quorum")
                    .containsEntry("x-message-ttl", 86_400_000);
        }
    }

    @Test
    void bindingsDirectTopicYDlqSegunLaTablaDelCaso() {
        assertThat(declarables.getDeclarablesByType(Binding.class)).hasSize(9);
        assertBinding("cmd.direct", "email.send", "q.cmd.email");
        assertBinding("cmd.topic", "email.*", "q.cmd.email");
        assertBinding("cmd.direct", "dispatch.ticket", "q.cmd.dispatch");
        assertBinding("cmd.topic", "dispatch.#", "q.cmd.dispatch");
        assertBinding("cmd.direct", "report.gen", "q.cmd.report");
        assertBinding("cmd.topic", "report.*", "q.cmd.report");
        assertBinding("cmd.dead.dlx", "email.dead", "q.cmd.email.dlq");
        assertBinding("cmd.dead.dlx", "dispatch.dead", "q.cmd.dispatch.dlq");
        assertBinding("cmd.dead.dlx", "report.dead", "q.cmd.report.dlq");
    }

    @Test
    void cadaFlujoSeEntregaEnSuEndpointInternoDeMsNotify() {
        assertThat(topologia.flujo("email").orElseThrow().destino())
                .isEqualTo("http://localhost:8083/internal/v1/notificaciones/email");
        assertThat(topologia.flujo("dispatch").orElseThrow().destino())
                .isEqualTo("http://localhost:8083/internal/v1/notificaciones/despacho");
        assertThat(topologia.flujo("report").orElseThrow().destino())
                .isEqualTo("http://localhost:8083/internal/v1/notificaciones/informe");
    }

    @Test
    void elPrefijoTopicSaleDelPatron() {
        assertThat(topologia.flujo("email").orElseThrow().prefijoTopic()).isEqualTo("email");
        assertThat(topologia.flujo("dispatch").orElseThrow().prefijoTopic()).isEqualTo("dispatch");
    }

    @Test
    void reintentosTransitoriosConfigurados() {
        assertThat(topologia.retry().maxAttempts()).isEqualTo(3);
    }

    private static void assertExchange(Exchange exchange, String tipo) {
        assertThat(exchange).isNotNull();
        assertThat(exchange.getType()).isEqualTo(tipo);
        assertThat(exchange.isDurable()).isTrue();
    }

    private static void assertColaConDlq(Queue cola, String routingKeyDlq) {
        assertThat(cola).isNotNull();
        assertThat(cola.isDurable()).isTrue();
        assertThat(cola.getArguments())
                .containsEntry("x-queue-type", "quorum")
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", routingKeyDlq);
    }

    private static void assertBinding(String exchange, String routingKey, String cola) {
        assertThat(declarables.getDeclarablesByType(Binding.class))
                .anySatisfy(b -> {
                    assertThat(b.getExchange()).isEqualTo(exchange);
                    assertThat(b.getRoutingKey()).isEqualTo(routingKey);
                    assertThat(b.getDestination()).isEqualTo(cola);
                    assertThat(b.getDestinationType()).isEqualTo(Binding.DestinationType.QUEUE);
                });
    }
}
