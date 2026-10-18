package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.dto.BindingRequestDTO;
import df.digitalfix_ms_admin_queues.dto.ExchangeRequestDTO;
import df.digitalfix_ms_admin_queues.dto.QueueRequestDTO;
import df.digitalfix_ms_admin_queues.enums.ExchangeType;
import df.digitalfix_ms_admin_queues.exception.AdminQueueException;
import df.digitalfix_ms_admin_queues.exception.ResourceNotFoundException;
import df.digitalfix_ms_admin_queues.service.RabbitAdminService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Service
@RequiredArgsConstructor
public class RabbitAdminServiceImpl implements RabbitAdminService {

    private final RabbitAdmin rabbitAdmin;

    // ===================================================================
    // GESTIÓN DE COLAS (QUEUES)
    // ===================================================================

    @Override
    public void createQueue(QueueRequestDTO dto) {
        log.info("Iniciando creación de cola: {}", dto.getQueueName());

        Map<String, Object> arguments = new HashMap<>();

        // Configuración de Dead Letter Exchange (DLX) si viene presente
        if (dto.getDeadLetterExchange() != null && !dto.getDeadLetterExchange().isBlank()) {
            arguments.put("x-dead-letter-exchange", dto.getDeadLetterExchange());
        }
        if (dto.getDeadLetterRoutingKey() != null && !dto.getDeadLetterRoutingKey().isBlank()) {
            arguments.put("x-dead-letter-routing-key", dto.getDeadLetterRoutingKey());
        }
        if (dto.getMessageTtl() != null && dto.getMessageTtl() > 0) {
            arguments.put("x-message-ttl", dto.getMessageTtl());
        }

        Queue queue = new Queue(
                dto.getQueueName(),
                dto.isDurable(),
                dto.isExclusive(),
                dto.isAutoDelete(),
                arguments
        );

        try {
            String declaredQueue = rabbitAdmin.declareQueue(queue);
            log.info("Cola creada exitosamente en RabbitMQ: {}", declaredQueue);
        } catch (Exception e) {
            log.error("Error al declarar la cola {}: {}", dto.getQueueName(), e.getMessage());
            throw new AdminQueueException("Fallo al crear la cola: " + dto.getQueueName(), e);
        }
    }

    @Override
    public boolean deleteQueue(String queueName, boolean unusedOnly, boolean emptyOnly) {
        log.info("Eliminando cola: {} (unusedOnly={}, emptyOnly={})", queueName, unusedOnly, emptyOnly);
        try {
            rabbitAdmin.deleteQueue(queueName, unusedOnly, emptyOnly);
            return true;
        } catch (Exception e) {
            log.error("Error al eliminar la cola {}: {}", queueName, e.getMessage());
            throw new AdminQueueException("Fallo al eliminar la cola: " + queueName, e);
        }
    }

    @Override
    public int purgeQueue(String queueName) {
        log.info("Purgando todos los mensajes de la cola: {}", queueName);
        try {
            int purgedMessages = rabbitAdmin.purgeQueue(queueName);
            log.info("Se purgaron {} mensajes de la cola {}", purgedMessages, queueName);
            return purgedMessages;
        } catch (Exception e) {
            log.error("Error al purgar la cola {}: {}", queueName, e.getMessage());
            throw new ResourceNotFoundException("No se pudo purgar la cola (es posible que no exista): " + queueName);
        }
    }

    @Override
    public Properties getQueueProperties(String queueName) {
        log.info("Consultando propiedades de la cola: {}", queueName);
        Properties props = rabbitAdmin.getQueueProperties(queueName);
        if (props == null) {
            throw new ResourceNotFoundException("La cola especificada no existe en el broker: " + queueName);
        }
        return props;
    }

    // ===================================================================
    // GESTIÓN DE EXCHANGES
    // ===================================================================

    @Override
    public void createExchange(ExchangeRequestDTO dto) {
        log.info("Iniciando creación de exchange: {} de tipo {}", dto.getExchangeName(), dto.getType());

        Exchange exchange = buildExchange(dto);

        try {
            rabbitAdmin.declareExchange(exchange);
            log.info("Exchange {} ({}) creado exitosamente", dto.getExchangeName(), dto.getType());
        } catch (Exception e) {
            log.error("Error al declarar el exchange {}: {}", dto.getExchangeName(), e.getMessage());
            throw new AdminQueueException("Fallo al crear el exchange: " + dto.getExchangeName(), e);
        }
    }

    @Override
    public boolean deleteExchange(String exchangeName) {
        log.info("Eliminando exchange: {}", exchangeName);
        try {
            return rabbitAdmin.deleteExchange(exchangeName);
        } catch (Exception e) {
            log.error("Error al eliminar el exchange {}: {}", exchangeName, e.getMessage());
            throw new AdminQueueException("Fallo al eliminar el exchange: " + exchangeName, e);
        }
    }

    // ===================================================================
    // GESTIÓN DE BINDINGS (ENLACES)
    // ===================================================================

    @Override
    public void declareBinding(BindingRequestDTO dto) {
        log.info("Enlazando cola '{}' con exchange '{}' mediante routingKey '{}'", 
                dto.getQueueName(), dto.getExchangeName(), dto.getRoutingKey());

        Binding binding = new Binding(
                dto.getQueueName(),
                Binding.DestinationType.QUEUE,
                dto.getExchangeName(),
                dto.getRoutingKey(),
                null
        );

        try {
            rabbitAdmin.declareBinding(binding);
            log.info("Binding creado exitosamente para cola {}", dto.getQueueName());
        } catch (Exception e) {
            log.error("Error al declarar binding: {}", e.getMessage());
            throw new AdminQueueException("Fallo al asociar la cola con el exchange", e);
        }
    }

    @Override
    public void removeBinding(BindingRequestDTO dto) {
        log.info("Removiendo enlace de cola '{}' con exchange '{}'", dto.getQueueName(), dto.getExchangeName());

        Binding binding = new Binding(
                dto.getQueueName(),
                Binding.DestinationType.QUEUE,
                dto.getExchangeName(),
                dto.getRoutingKey(),
                null
        );

        try {
            rabbitAdmin.removeBinding(binding);
            log.info("Binding removido exitosamente");
        } catch (Exception e) {
            log.error("Error al remover binding: {}", e.getMessage());
            throw new AdminQueueException("Fallo al eliminar el binding", e);
        }
    }

    // Método privado para construir el tipo de Exchange correcto según el Enum
    private Exchange buildExchange(ExchangeRequestDTO dto) {
        String typeUpper = dto.getType().toUpperCase();
        
        if (ExchangeType.DIRECT.name().equals(typeUpper)) {
            return new DirectExchange(dto.getExchangeName(), dto.isDurable(), dto.isAutoDelete());
        } else if (ExchangeType.TOPIC.name().equals(typeUpper)) {
            return new TopicExchange(dto.getExchangeName(), dto.isDurable(), dto.isAutoDelete());
        } else if (ExchangeType.FANOUT.name().equals(typeUpper)) {
            return new FanoutExchange(dto.getExchangeName(), dto.isDurable(), dto.isAutoDelete());
        } else if (ExchangeType.HEADERS.name().equals(typeUpper)) {
            return new HeadersExchange(dto.getExchangeName(), dto.isDurable(), dto.isAutoDelete());
        } else {
            throw new AdminQueueException("Tipo de exchange no soportado: " + dto.getType());
        }
    }
}
