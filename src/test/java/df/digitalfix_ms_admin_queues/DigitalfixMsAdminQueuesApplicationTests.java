package df.digitalfix_ms_admin_queues;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestPropertySource(properties = {"AZURE_TENANT_ID=test-tenant", "AZURE_CLIENT_ID=test-client"})
class DigitalfixMsAdminQueuesApplicationTests {

	@MockitoBean
	private JwtDecoder jwtDecoder;

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	// Un listener por flujo más el de cuarentena, con los mismos ids que usa el panel de Mensajería.
	@Test
	void registraLosListenersDeTodosLosFlujos() {
		assertThat(registry.getListenerContainerIds())
				.containsExactlyInAnyOrder("listener-email", "listener-dispatch", "listener-report", "listener-dlq");
	}

}
