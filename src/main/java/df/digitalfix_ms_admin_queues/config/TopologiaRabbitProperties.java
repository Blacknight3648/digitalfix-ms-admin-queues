package df.digitalfix_ms_admin_queues.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Única fuente de la topología RabbitMQ de DigitalFix ({@code app.rabbitmq} en application.yaml).
 * ms-admin-queues es el dueño de toda la mensajería: declara exchanges, colas y bindings, publica
 * los eventos que le envían los productores y entrega cada mensaje al microservicio de destino.
 * Ningún otro microservicio conoce un nombre de cola, exchange o routing key.
 * <p>
 * Se valida al arrancar: si falta un nombre, el servicio no levanta (mejor que declarar una cola
 * vacía o con un nombre distinto al esperado).
 *
 * @param exchanges los tres exchanges del caso: {@code cmd.direct}, {@code cmd.topic} y {@code cmd.dead.dlx}
 * @param flujos    un {@link Flujo} por caso de uso, por nombre (email, dispatch, report). Agregar un
 *                  flujo aquí crea su cola, su DLQ, sus bindings, su listener y su entrega HTTP.
 * @param dlqTtl    cuánto se conserva un mensaje en una DLQ antes de descartarse
 * @param retry     reintentos de errores transitorios antes de derivar a la DLQ
 * @param entrega   tiempos de espera de la entrega HTTP a los microservicios de destino
 * @param publicacion tiempo máximo que se espera la confirmación del broker al publicar
 */
@Validated
@ConfigurationProperties(prefix = "app.rabbitmq")
public record TopologiaRabbitProperties(
        @Valid @NotNull Exchanges exchanges,
        @Valid @NotEmpty Map<String, Flujo> flujos,
        @NotNull Duration dlqTtl,
        @Valid @NotNull Reintentos retry,
        @Valid @NotNull Entrega entrega,
        @Valid @NotNull Publicacion publicacion
) {

    /** Busca un flujo por su nombre (el {@code {flujo}} de la API de publicación). */
    public Optional<Flujo> flujo(String nombre) {
        return Optional.ofNullable(flujos.get(nombre));
    }

    public record Exchanges(
            /* Comandos punto a punto: routing key exacta (email.send, dispatch.ticket, report.gen). */
            @NotBlank String direct,
            /* Mismos comandos por patrón (email.*, dispatch.#, report.*). */
            @NotBlank String topic,
            /* Dead Letter Exchange: recibe los NACK sin requeue y los enruta a cada DLQ. */
            @NotBlank String dlx
    ) {
    }

    /**
     * Ruta completa de un caso de uso. El productor publica en {@code cmd.direct} con
     * {@code routingKey} (o en {@code cmd.topic} con {@code <prefijo>.<subtipo>}, que calza con
     * {@code patronTopic}); el mensaje llega a {@code cola} y el listener de ms-admin-queues lo
     * entrega por HTTP en {@code destino}. Si se rechaza, el DLX lo lleva a {@code dlq} con
     * {@code routingKeyDlq}.
     */
    public record Flujo(
            @NotBlank String cola,
            @NotBlank String dlq,
            @NotBlank String routingKey,
            @NotBlank @Pattern(regexp = "^[a-z0-9_-]+\\.[*#]$",
                    message = "El patrón topic debe ser <prefijo>.* o <prefijo>.#")
            String patronTopic,
            @NotBlank String routingKeyDlq,
            /* URL del endpoint interno del microservicio que procesa el mensaje. */
            @NotBlank @Pattern(regexp = "^https?://.+", message = "El destino debe ser una URL http(s)")
            String destino
    ) {

        /** Prefijo del topic: de {@code email.*} se publica {@code email.<subtipo>}. */
        public String prefijoTopic() {
            return patronTopic.substring(0, patronTopic.lastIndexOf('.'));
        }
    }

    public record Reintentos(@Min(0) int maxAttempts) {
    }

    public record Entrega(@NotNull Duration connectTimeout, @NotNull Duration readTimeout) {
    }

    public record Publicacion(@NotNull Duration confirmTimeout) {
    }
}
