package com.factoring.commons.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * {@link PublicadorOutbox} sobre Kafka, con un {@link KafkaTemplate} String/String propio (no el del servicio): el
 * mensaje sale siempre igual, sin headers de tipo de Spring, con las cabeceras del sobre.
 *
 * <p>Los tipos declarados con {@link TipoDeEvento.Builder#compatibleConJsonSerializer(String)} (desde 0.6.1) salen en
 * cambio con el formato del {@link JsonSerializer} de Spring Kafka: el valor es solo el payload, serializado con un
 * {@code JsonSerializer} por defecto (mismos bytes que un productor que lo use), y además de las cabeceras del sobre
 * va {@value TipoDeEvento#CABECERA_TIPO_SPRING}. El payload se lee del sobre guardado con {@link OutboxJson}: su
 * clase tiene que poder reconstruirse desde su propio JSON (un record o un POJO con Jackson).</p>
 */
public class KafkaPublicadorOutbox implements PublicadorOutbox, DisposableBean {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Map<String, TipoDeEvento<?>> compatibles = new HashMap<>();
    private final JsonSerializer<Object> serializer = new JsonSerializer<>();

    /**
     * Todos los tipos salen como sobre (la forma de 0.6.0).
     *
     * @param kafkaTemplate template String/String exclusivo del outbox
     */
    public KafkaPublicadorOutbox(final KafkaTemplate<String, String> kafkaTemplate) {
        this(kafkaTemplate, null);
    }

    /**
     * @param kafkaTemplate template String/String exclusivo del outbox
     * @param tipos         tipos del servicio; los compatibles con {@code JsonSerializer} salen con ese formato
     *                      ({@code null}: todos como sobre)
     */
    public KafkaPublicadorOutbox(final KafkaTemplate<String, String> kafkaTemplate, final TiposDeEvento tipos) {
        this.kafkaTemplate = kafkaTemplate;
        if (tipos != null) {
            for (final String nombre : tipos.nombres()) {
                final TipoDeEvento<?> tipo = tipos.requerir(nombre);
                if (tipo.compatibleConJsonSerializer()) {
                    compatibles.put(nombre, tipo);
                }
            }
        }
    }

    @Override
    public void publicarConfirmado(final String topic, final String clave, final String valor,
            final Map<String, String> cabeceras, final Duration espera) {
        final String eventId = cabeceras.get(EventoOutbox.CABECERA_EVENT_ID);
        final TipoDeEvento<?> compatible = compatibles.get(cabeceras.get(EventoOutbox.CABECERA_TIPO));
        try {
            String cuerpo = valor;
            if (compatible != null) {
                cuerpo = soloPayload(topic, valor, compatible);
            }
            final ProducerRecord<String, String> registro = new ProducerRecord<>(topic, clave, cuerpo);
            cabeceras.forEach((nombre, contenido) -> registro.headers().add(nombre,
                    contenido.getBytes(StandardCharsets.UTF_8)));
            if (compatible != null) {
                registro.headers().add(TipoDeEvento.CABECERA_TIPO_SPRING,
                        compatible.idTipoCompatible().getBytes(StandardCharsets.UTF_8));
            }
            kafkaTemplate.send(registro).get(espera.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxNoPublicadoException(eventId, e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new OutboxNoPublicadoException(eventId, e);
        }
    }

    /** {@code delivery.timeout.ms} efectivo del productor de este publicador. */
    @Override
    public Optional<Duration> plazoDeEntrega() {
        return Optional.of(ProductorOutbox.plazoDeEntrega(kafkaTemplate.getProducerFactory().getConfigurationProperties()));
    }

    /**
     * El payload del sobre con los bytes del {@code JsonSerializer}. JSON de Jackson siempre es UTF-8 válido, así que
     * pasarlo a String y que el {@code StringSerializer} lo vuelva a codificar deja los mismos bytes.
     */
    private String soloPayload(final String topic, final String sobre, final TipoDeEvento<?> tipo) {
        final byte[] bytes = serializer.serialize(topic, OutboxJson.deserializar(sobre, tipo.clase()).payload());
        if (bytes == null) {
            throw new IllegalArgumentException("El evento " + tipo.nombre() + " no tiene payload");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void destroy() {
        kafkaTemplate.destroy();
    }
}
