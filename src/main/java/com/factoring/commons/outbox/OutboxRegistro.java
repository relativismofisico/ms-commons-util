package com.factoring.commons.outbox;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento;
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort;
import com.factoring.commons.mdc.MdcConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * {@link OutboxPort} sobre el almacén del outbox ({@link AuditoriaOutboxPort}, JDBC o Mongo): guarda el sobre como un
 * evento con {@code tipo}, que el relay publica con sus cabeceras.
 */
public class OutboxRegistro implements OutboxPort {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRegistro.class);

    private final AuditoriaOutboxPort almacen;
    private final TiposDeEvento tipos;
    private final String servicioOrigen;
    private final Clock reloj;
    private final boolean soloDrenar;
    private final ConcurrentMap<String, LongAdder> duplicados = new ConcurrentHashMap<>();

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
        final TipoDeEvento<?> declarado = declarado(tipo, payload);
        final EventoOutbox<T> evento = sobre(declarado, UUID.randomUUID().toString(), payload);
        almacen.guardar(fila(declarado, evento));
        return evento;
    }

    @Override
    public <T> Optional<EventoOutbox<T>> registrar(final String tipo, final String eventId, final T payload) {
        final TipoDeEvento<?> declarado = declarado(tipo, payload);
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("El eventId del evento " + tipo + " está vacío");
        }
        if (eventId.length() > EventoOutbox.LONGITUD_MAXIMA_EVENT_ID) {
            throw new IllegalArgumentException("El eventId del evento " + tipo + " pasa de "
                    + EventoOutbox.LONGITUD_MAXIMA_EVENT_ID + " caracteres");
        }
        final EventoOutbox<T> evento = sobre(declarado, eventId, payload);
        if (almacen.guardarSiNoExiste(fila(declarado, evento))) {
            return Optional.of(evento);
        }
        duplicados.computeIfAbsent(tipo, t -> new LongAdder()).increment();
        LOG.warn("Evento de outbox duplicado: ya había un evento {} con eventId {}; no se escribe otro", tipo, eventId);
        return Optional.empty();
    }

    /**
     * Registros descartados por {@code eventId} repetido desde que arrancó el servicio (métrica
     * {@code outbox.duplicados}).
     *
     * @param tipo nombre del tipo
     * @return cuántos
     */
    public long duplicados(final String tipo) {
        final LongAdder contador = duplicados.get(tipo);
        if (contador == null) {
            return 0;
        }
        return contador.sum();
    }

    private TipoDeEvento<?> declarado(final String tipo, final Object payload) {
        final TipoDeEvento<?> declarado = tipos.requerir(tipo);
        if (soloDrenar) {
            throw new OutboxSoloDrenarException(tipo);
        }
        if (!declarado.clase().isInstance(payload)) {
            throw new IllegalArgumentException("El payload del evento " + tipo + " no es " + declarado.clase().getName());
        }
        return declarado;
    }

    private <T> EventoOutbox<T> sobre(final TipoDeEvento<?> declarado, final String eventId, final T payload) {
        final Instant ahora = reloj.instant().truncatedTo(ChronoUnit.MILLIS);
        return new EventoOutbox<>(eventId, declarado.nombre(), declarado.version(), servicioOrigen, ahora,
                MDC.get(MdcConstants.REQUEST_ID), declarado.claveDe(payload), payload);
    }

    private static AuditoriaOutboxEvento fila(final TipoDeEvento<?> declarado, final EventoOutbox<?> evento) {
        return AuditoriaOutboxEvento.nuevo(evento.eventId(), evento.clave(), declarado.topic(),
                OutboxJson.serializar(evento), evento.ocurridoEn()).conTipo(declarado.nombre());
    }
}
