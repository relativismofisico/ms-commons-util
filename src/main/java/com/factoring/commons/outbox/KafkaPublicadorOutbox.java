package com.factoring.commons.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * {@link PublicadorOutbox} sobre Kafka, con un {@link KafkaTemplate} String/String propio (no el del servicio): el
 * mensaje sale siempre igual, sin headers de tipo de Spring, con las cabeceras del sobre.
 */
public class KafkaPublicadorOutbox implements PublicadorOutbox, DisposableBean {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * @param kafkaTemplate template String/String exclusivo del outbox
     */
    public KafkaPublicadorOutbox(final KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publicarConfirmado(final String topic, final String clave, final String valor,
            final Map<String, String> cabeceras, final Duration espera) {
        final ProducerRecord<String, String> registro = new ProducerRecord<>(topic, clave, valor);
        cabeceras.forEach((nombre, contenido) -> registro.headers().add(nombre,
                contenido.getBytes(StandardCharsets.UTF_8)));
        final String eventId = cabeceras.get(EventoOutbox.CABECERA_EVENT_ID);
        try {
            kafkaTemplate.send(registro).get(espera.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxNoPublicadoException(eventId, e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new OutboxNoPublicadoException(eventId, e);
        }
    }

    @Override
    public void destroy() {
        kafkaTemplate.destroy();
    }
}
