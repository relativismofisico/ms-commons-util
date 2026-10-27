package com.factoring.commons.auditoria;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * {@link AuditoriaPublisher} sobre Kafka: JSON de {@link AuditoriaJson} (sin headers de tipo) en el topic de
 * auditoría, con clave {@link AuditoriaAccionEvent#claveKafka()} para conservar el orden por entidad.
 *
 * <p>Usa un {@link KafkaTemplate} propio, no el del servicio: cada servicio configura sus templates con serializers
 * distintos, y el evento de auditoría tiene que salir siempre igual.</p>
 *
 * <p>Un evento que no se pudo publicar se escribe completo solo en el logger dedicado
 * {@value #LOGGER_NO_PUBLICADO}: en los servicios sin outbox es la única copia para reenviarlo, pero lleva ip, actor y
 * detalle, que no deben quedar en el log general. Cada servicio dirige ese logger a un archivo de acceso restringido
 * (additivity=false); en el log general solo quedan los identificadores.</p>
 */
@Slf4j
public class KafkaAuditoriaPublisher implements AuditoriaPublisher, DisposableBean {

    /** Nombre fijo del logger con el evento completo no publicado; los logback de los servicios lo referencian. */
    public static final String LOGGER_NO_PUBLICADO = "AUDITORIA_NO_PUBLICADO";

    private static final Logger LOG_NO_PUBLICADO = LoggerFactory.getLogger(LOGGER_NO_PUBLICADO);

    /** Espera máxima de la confirmación en {@link #publicarConfirmado}: por encima de delivery.timeout.ms (120 s). */
    private static final long ESPERA_CONFIRMACION_S = 130;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;
    private final String servicioOrigen;

    /**
     * @param kafkaTemplate  template String/String exclusivo de la auditoría
     * @param topic          topic de auditoría
     * @param servicioOrigen {@code spring.application.name}, para los eventos que no lo informen
     */
    public KafkaAuditoriaPublisher(final KafkaTemplate<String, String> kafkaTemplate, final String topic,
            final String servicioOrigen) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.servicioOrigen = servicioOrigen;
    }

    @Override
    public void publicar(final AuditoriaAccionEvent evento) {
        final AuditoriaAccionEvent completo = completar(evento);
        final String json = AuditoriaJson.serializar(completo);
        try {
            kafkaTemplate.send(topic, completo.claveKafka(), json).whenComplete((resultado, error) -> {
                if (error != null) {
                    noPublicado(completo, json, error);
                }
            });
        } catch (RuntimeException e) {
            // send() también puede fallar de forma síncrona (metadata del topic sin broker, max.block.ms).
            noPublicado(completo, json, e);
        }
    }

    @Override
    public void publicarConfirmado(final AuditoriaAccionEvent evento) {
        final AuditoriaAccionEvent completo = completar(evento);
        try {
            kafkaTemplate.send(topic, completo.claveKafka(), AuditoriaJson.serializar(completo))
                    .get(ESPERA_CONFIRMACION_S, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AuditoriaNoPublicadaException(completo.eventId(), e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new AuditoriaNoPublicadaException(completo.eventId(), e);
        }
    }

    private AuditoriaAccionEvent completar(final AuditoriaAccionEvent evento) {
        if (evento.servicioOrigen() == null || evento.servicioOrigen().isBlank()) {
            return evento.conServicioOrigen(servicioOrigen);
        }
        return evento;
    }

    /**
     * Log general con un marcador fijo y solo los identificadores; el evento completo (lo que haga falta para
     * reenviarlo a mano) va únicamente al logger dedicado {@value #LOGGER_NO_PUBLICADO}.
     */
    private static void noPublicado(final AuditoriaAccionEvent evento, final String json, final Throwable error) {
        log.error("[AUDITORIA][NO_PUBLICADO] eventId={} accion={} entidad={} servicioOrigen={} (evento completo en "
                        + "el logger " + LOGGER_NO_PUBLICADO + ")", evento.eventId(), evento.accion(),
                entidadDe(evento), evento.servicioOrigen(), error);
        LOG_NO_PUBLICADO.error("eventId={} evento={}", evento.eventId(), json);
    }

    /** {@code tipo:id} de la entidad auditada; {@code null} si el evento llegó sin entidad. */
    private static String entidadDe(final AuditoriaAccionEvent evento) {
        final AuditoriaAccionEvent.Entidad entidad = evento.entidad();
        if (entidad == null) {
            return null;
        }
        return entidad.tipo() + ":" + entidad.id();
    }

    @Override
    public void destroy() {
        kafkaTemplate.destroy();
    }
}
