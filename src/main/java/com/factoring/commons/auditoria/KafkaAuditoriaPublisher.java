package com.factoring.commons.auditoria;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * {@link AuditoriaPublisher} sobre Kafka: JSON de {@link AuditoriaJson} (sin headers de tipo) en el topic de
 * auditoría, con clave {@link AuditoriaAccionEvent#claveKafka()} para conservar el orden por entidad.
 *
 * <p>Usa un {@link KafkaTemplate} propio, no el del servicio: cada servicio configura sus templates con serializers
 * distintos, y el evento de auditoría tiene que salir siempre igual.</p>
 */
@Slf4j
public class KafkaAuditoriaPublisher implements AuditoriaPublisher, DisposableBean {

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
        AuditoriaAccionEvent completo = evento;
        if (completo.servicioOrigen() == null || completo.servicioOrigen().isBlank()) {
            completo = completo.conServicioOrigen(servicioOrigen);
        }
        final String json = AuditoriaJson.serializar(completo);
        final String eventId = completo.eventId();
        try {
            kafkaTemplate.send(topic, completo.claveKafka(), json).whenComplete((resultado, error) -> {
                if (error != null) {
                    noPublicado(eventId, json, error);
                }
            });
        } catch (RuntimeException e) {
            // send() también puede fallar de forma síncrona (metadata del topic sin broker, max.block.ms).
            noPublicado(eventId, json, e);
        }
    }

    /** Línea de log con un marcador fijo y el evento completo: lo que haga falta para reenviarlo a mano. */
    private static void noPublicado(final String eventId, final String json, final Throwable error) {
        log.error("[AUDITORIA][NO_PUBLICADO] eventId={} evento={}", eventId, json, error);
    }

    @Override
    public void destroy() {
        kafkaTemplate.destroy();
    }
}
