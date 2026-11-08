package com.factoring.commons.outbox;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento;
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort;
import com.factoring.commons.mdc.MdcConstants;
import org.slf4j.MDC;

/**
 * {@link OutboxPort} sobre el almacén del outbox ({@link AuditoriaOutboxPort}, JDBC o Mongo): guarda el sobre como un
 * evento con {@code tipo}, que el relay publica con sus cabeceras.
 */
public class OutboxRegistro implements OutboxPort {

    private final AuditoriaOutboxPort almacen;
    private final TiposDeEvento tipos;
    private final String servicioOrigen;
    private final Clock reloj;
    private final boolean soloDrenar;

    /**
     * @param almacen        almacén del outbox
     * @param tipos          tipos declarados, con los topics ya resueltos
     * @param servicioOrigen {@code spring.application.name}
     * @param reloj          reloj de {@code ocurridoEn}
     * @param soloDrenar     si el outbox está en modo "solo drenar" (rechaza eventos nuevos)
     */
    public OutboxRegistro(final AuditoriaOutboxPort almacen, final TiposDeEvento tipos, final String servicioOrigen,
            final Clock reloj, final boolean soloDrenar) {
        this.almacen = almacen;
        this.tipos = tipos;
        this.servicioOrigen = servicioOrigen;
        this.reloj = reloj;
        this.soloDrenar = soloDrenar;
    }

    @Override
    public <T> EventoOutbox<T> registrar(final String tipo, final T payload) {
        final TipoDeEvento<?> declarado = tipos.requerir(tipo);
        if (soloDrenar) {
            throw new OutboxSoloDrenarException(tipo);
        }
        if (!declarado.clase().isInstance(payload)) {
            throw new IllegalArgumentException("El payload del evento " + tipo + " no es " + declarado.clase().getName());
        }
        final Instant ahora = reloj.instant().truncatedTo(ChronoUnit.MILLIS);
        final EventoOutbox<T> evento = new EventoOutbox<>(UUID.randomUUID().toString(), tipo, declarado.version(),
                servicioOrigen, ahora, MDC.get(MdcConstants.REQUEST_ID), declarado.claveDe(payload), payload);
        almacen.guardar(AuditoriaOutboxEvento.nuevo(evento.eventId(), evento.clave(), declarado.topic(),
                OutboxJson.serializar(evento), ahora).conTipo(tipo));
        return evento;
    }
}
