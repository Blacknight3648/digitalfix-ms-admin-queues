package df.digitalfix_ms_admin_queues.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitAdminConfig {

    /**
     * Declara el Bean principal de RabbitAdmin.
     * Utiliza la fábrica de conexiones inyectada dinámicamente desde application.yaml
     * para permitir la gestión en tiempo de ejecución de la topología de RabbitMQ.
     */
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        // Habilita el arranque automático para declarar los recursos al iniciar
        rabbitAdmin.setAutoStartup(true);
        return rabbitAdmin;
    }
}
