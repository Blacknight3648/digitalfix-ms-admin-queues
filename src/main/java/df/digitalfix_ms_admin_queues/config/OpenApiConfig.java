package df.digitalfix_ms_admin_queues.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI adminQueuesOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("DigitalFix — ms-admin-queues")
                        .version("1.0.0")
                        .description("Mensajería centralizada de DigitalFix: topología de RabbitMQ (colas, exchanges, "
                                + "bindings), publicación de eventos de los productores, entrega HTTP a los "
                                + "microservicios de destino y control de listeners. La administración se consume a "
                                + "través del BFF (http://localhost:8090), que exige un JWT de Entra ID con rol "
                                + "SUPERADMIN; /internal/** solo es accesible dentro de la red digitalfix-net."))
                .servers(List.of(
                        new Server().url("http://localhost:8090").description("BFF Gateway (requiere Bearer token)"),
                        new Server().url("http://localhost:8084").description("Acceso directo (solo desarrollo local)")));
    }
}
