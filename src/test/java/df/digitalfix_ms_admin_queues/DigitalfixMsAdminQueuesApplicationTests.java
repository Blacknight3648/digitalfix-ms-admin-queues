package df.digitalfix_ms_admin_queues;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class DigitalfixMsAdminQueuesApplicationTests {

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	// Un listener por flujo más el de cuarentena, con los mismos ids que usa el panel de Mensajería.
	@Test
	void registraLosListenersDeTodosLosFlujos() {
		assertThat(registry.getListenerContainerIds())
				.containsExactlyInAnyOrder("listener-email", "listener-dispatch", "listener-report", "listener-dlq");
	}

}
