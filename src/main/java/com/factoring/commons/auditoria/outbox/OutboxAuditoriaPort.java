package com.factoring.commons.auditoria.outbox;

import java.time.Clock;

import com.factoring.commons.auditoria.AuditoriaAccionEvent;
import com.factoring.commons.auditoria.AuditoriaJson;
import com.factoring.commons.auditoria.AuditoriaPort;
import com.factoring.commons.auditoria.AuditoriaPublisher;

/**
 * {@link AuditoriaPort} respaldado por el outbox: guarda el evento en el almacén del servicio participando en la
 * transacción activa, así el evento se confirma o se revierte junto con el cambio de negocio. Sin transacción activa
 * se escribe igual y queda confirmado en el momento (duradero, aunque ya no atado a otra escritura). Si la escritura
 * falla, la excepción se propaga.
 *
 * <p>En modo "solo drenar" ({@code auditoria.outbox.solo-drenar=true}) no escribe en el outbox: publica directo con
 * {@link AuditoriaPublisher#publicar}, el camino sin outbox, mientras el relay termina de drenar lo pendiente.</p>
 */
public class OutboxAuditoriaPort implements AuditoriaPort {

    private final AuditoriaOutboxPort outbox;
    private final String topic;
    private final String servicioOrigen;
    private final Clock reloj;
    private final AuditoriaPublisher directo;

    /**
     * @param outbox         almacén del outbox
     * @param topic          topic de auditoría ({@code auditoria.topic})
     * @param servicioOrigen {@code spring.application.name}, para los eventos que no lo informen
     * @param reloj          reloj de las fechas del outbox
     */
    public OutboxAuditoriaPort(final AuditoriaOutboxPort outbox, final String topic, final String servicioOrigen,
            final Clock reloj) {
        this(outbox, topic, servicioOrigen, reloj, null);
    }

    /**
     * @param outbox         almacén del outbox
     * @param topic          topic de auditoría ({@code auditoria.topic})
     * @param servicioOrigen {@code spring.application.name}, para los eventos que no lo informen
     * @param reloj          reloj de las fechas del outbox
     * @param directo        publicador del modo "solo drenar" ({@code null}: el outbox acepta eventos nuevos)
     */
    public OutboxAuditoriaPort(final AuditoriaOutboxPort outbox, final String topic, final String servicioOrigen,
            final Clock reloj, final AuditoriaPublisher directo) {
        this.directo = directo;
        this.outbox = outbox;
        this.topic = topic;
        this.servicioOrigen = servicioOrigen;
        this.reloj = reloj;
    }

    @Override
    public void registrar(final AuditoriaAccionEvent evento) {
        AuditoriaAccionEvent completo = evento;
        if (evento.servicioOrigen() == null || evento.servicioOrigen().isBlank()) {
            completo = evento.conServicioOrigen(servicioOrigen);
        }
        if (directo != null) {
            directo.publicar(completo);
            return;
        }
        outbox.guardar(AuditoriaOutboxEvento.nuevo(completo.eventId(), completo.claveKafka(), topic,
                AuditoriaJson.serializar(completo), reloj.instant()));
    }
}
