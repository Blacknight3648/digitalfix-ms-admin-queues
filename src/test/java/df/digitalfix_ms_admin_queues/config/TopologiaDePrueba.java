package df.digitalfix_ms_admin_queues.config;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Topología leída del application.yaml real, sin levantar Spring ni RabbitMQ. */
public final class TopologiaDePrueba {

    private TopologiaDePrueba() {
    }

    public static TopologiaRabbitProperties cargar() {
        try {
            StandardEnvironment env = new StandardEnvironment();
            new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yaml"))
                    .forEach(env.getPropertySources()::addLast);
            return Binder.get(env).bind("app.rabbitmq", TopologiaRabbitProperties.class).get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
