package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.factoring.commons.auditoria.AuditoriaAccionEvent;
import com.factoring.commons.auditoria.AuditoriaJson;
import com.factoring.commons.auditoria.AuditoriaPublisher;
import lombok.extern.slf4j.Slf4j;

/**
 * Publica en Kafka los eventos del outbox de auditoría. En cada ciclo:
 * <ol>
 *   <li>devuelve a reintento (contando un intento) los eventos cuyo lease venció: el relay que los tenía cayó a
 *       medias;</li>
 *   <li>reclama un lote con lease ({@link AuditoriaOutboxPort#tomarLote}), fuera de cualquier transacción de
 *       negocio;</li>
 *   <li>publica cada evento con {@link AuditoriaPublisher#publicarConfirmado(AuditoriaAccionEvent, String, Duration)} y
 *       lo marca ENVIADO solo tras el ack. Si falla, programa un reintento con backoff exponencial o, agotados los
 *       intentos, lo deja FALLIDO; y libera sin publicar el resto de su clave en el lote, para no desordenarla.</li>
 * </ol>
 *
 * <p>Varias instancias pueden correr a la vez: el almacén nunca entrega el mismo evento a dos relays. Si una cae entre
 * el ack y el marcado, el evento sale dos veces con el mismo {@code eventId} y el consumidor descarta el duplicado.</p>
 */
@Slf4j
public class OutboxAuditoriaRelay {

    private final AuditoriaOutboxPort outbox;
    private final AuditoriaPublisher publicador;
    private final AuditoriaOutboxProperties propiedades;
    private final Clock reloj;

    /**
     * @param outbox      almacén del outbox
     * @param publicador  publicador de auditoría (espera el ack)
     * @param propiedades {@code auditoria.outbox.*}
     * @param reloj       reloj de los leases y reintentos
     */
    public OutboxAuditoriaRelay(final AuditoriaOutboxPort outbox, final AuditoriaPublisher publicador,
            final AuditoriaOutboxProperties propiedades, final Clock reloj) {
        this.outbox = outbox;
        this.publicador = publicador;
        this.propiedades = propiedades;
        this.reloj = reloj;
    }

    /**
     * Un ciclo completo del relay.
     *
     * @return eventos publicados y marcados ENVIADO
     */
    public int ejecutarCiclo() {
        for (final AuditoriaOutboxEvento vencido : outbox.buscarLeasesVencidos(reloj.instant(),
                propiedades.getTamanoLote())) {
            registrarFallo(vencido, ErrorSaneado.LEASE_VENCIDO);
        }
        final Instant ahora = reloj.instant();
        final List<AuditoriaOutboxEvento> lote = outbox.tomarLote(propiedades.getTamanoLote(), ahora,
                ahora.plus(propiedades.getLease()));
        final Set<String> clavesDetenidas = new HashSet<>();
        int enviados = 0;
        boolean leaseAgotado = false;
        for (final AuditoriaOutboxEvento evento : lote) {
            leaseAgotado = leaseAgotado || sinTiempoParaPublicar(evento);
            if (leaseAgotado || clavesDetenidas.contains(evento.claveKafka())) {
                outbox.liberar(evento);
            } else if (publicar(evento)) {
                enviados++;
            } else {
                clavesDetenidas.add(evento.claveKafka());
            }
        }
        return enviados;
    }

    /**
     * {@link #ejecutarCiclo()} sin dejar escapar excepciones (para el planificador): un fallo del almacén se registra
     * y el ciclo siguiente lo vuelve a intentar.
     */
    public void ejecutarCicloSeguro() {
        try {
            ejecutarCiclo();
        } catch (RuntimeException e) {
            log.error("[AUDITORIA][OUTBOX] ciclo del relay fallido: {}", ErrorSaneado.de(e));
        }
    }

    /** Si ya no queda lease para esperar un ack completo, el evento no se publica: otro relay lo tomaría a la vez. */
    private boolean sinTiempoParaPublicar(final AuditoriaOutboxEvento evento) {
        return reloj.instant().plus(propiedades.getTimeoutAck()).isAfter(evento.leaseHasta());
    }

    /** Publica y marca; devuelve falso si falló (la clave queda detenida en este lote). */
    private boolean publicar(final AuditoriaOutboxEvento evento) {
        final AuditoriaAccionEvent accion;
        try {
            accion = AuditoriaJson.deserializar(evento.payload());
        } catch (IllegalArgumentException e) {
            // Reintentar no lo arregla: queda para revisión manual.
            fallido(evento, evento.intentos() + 1, ErrorSaneado.PAYLOAD_ILEGIBLE);
            return false;
        }
        try {
            publicador.publicarConfirmado(accion, evento.topic(), propiedades.getTimeoutAck());
        } catch (RuntimeException e) {
            registrarFallo(evento, ErrorSaneado.de(e));
            return false;
        }
        outbox.marcarEnviado(evento.id(), reloj.instant());
        return true;
    }

    private void registrarFallo(final AuditoriaOutboxEvento evento, final String error) {
        final int intentos = evento.intentos() + 1;
        if (intentos >= propiedades.getMaxIntentos()) {
            fallido(evento, intentos, error);
            return;
        }
        final Duration espera = backoff(intentos);
        if (outbox.marcarReintento(evento, intentos, reloj.instant().plus(espera), error)) {
            log.warn("[AUDITORIA][OUTBOX_REINTENTO] id={} eventId={} intentos={} espera={} error={}", evento.id(),
                    evento.eventId(), intentos, espera, error);
        }
    }

    private void fallido(final AuditoriaOutboxEvento evento, final int intentos, final String error) {
        if (outbox.marcarFallido(evento, intentos, error)) {
            log.error("[AUDITORIA][OUTBOX_FALLIDO] id={} eventId={} claveKafka={} intentos={} error={} (bloquea los "
                            + "eventos siguientes de su clave hasta revisarlo)", evento.id(), evento.eventId(),
                    evento.claveKafka(), intentos, error);
        }
    }

    /**
     * Espera antes del intento siguiente: {@code backoffBase * 2^(intentos-1)}, con tope en {@code backoffMax}.
     *
     * @param intentos intentos fallidos, ya contando el último
     * @return la espera
     */
    Duration backoff(final int intentos) {
        final Duration base = propiedades.getBackoffBase();
        final Duration maximo = propiedades.getBackoffMax();
        Duration espera = base;
        for (int i = 1; i < intentos && espera.compareTo(maximo) < 0; i++) {
            espera = espera.multipliedBy(2);
        }
        if (espera.compareTo(maximo) > 0) {
            return maximo;
        }
        return espera;
    }
}
