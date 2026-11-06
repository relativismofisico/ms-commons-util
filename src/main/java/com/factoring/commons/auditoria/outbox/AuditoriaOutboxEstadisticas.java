package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

/**
 * Último {@link ResumenOutbox} leído del almacén, para las métricas. Lo refresca el planificador cada
 * {@code auditoria.outbox.intervalo-metricas}, y solo si alguien lo usa (las gauges de Micrometer lo activan): así un
 * servicio sin Micrometer no consulta el almacén de más. Leer las gauges nunca consulta la base.
 */
@Slf4j
public class AuditoriaOutboxEstadisticas {

    private static final double MILIS_POR_SEGUNDO = 1000.0;

    private final AuditoriaOutboxPort outbox;
    private final Clock reloj;
    private final AtomicBoolean activa = new AtomicBoolean();
    private final AtomicLong pendientes = new AtomicLong();
    private final AtomicLong fallidos = new AtomicLong();
    private final AtomicLong masAntiguoEpochMs = new AtomicLong(-1);

    /**
     * @param outbox almacén del outbox
     * @param reloj  reloj de la edad del pendiente más antiguo
     */
    public AuditoriaOutboxEstadisticas(final AuditoriaOutboxPort outbox, final Clock reloj) {
        this.outbox = outbox;
        this.reloj = reloj;
    }

    /** Empieza a refrescar las estadísticas (lo llama quien las publica). */
    public void activar() {
        activa.set(true);
    }

    /**
     * Si alguien las usa.
     *
     * @return verdadero tras {@link #activar()}
     */
    public boolean isActiva() {
        return activa.get();
    }

    /** Lee el resumen del almacén, si están activas; un fallo se registra y se conservan los valores anteriores. */
    public void actualizarSeguro() {
        if (!activa.get()) {
            return;
        }
        try {
            final ResumenOutbox resumen = outbox.resumen();
            pendientes.set(resumen.pendientes());
            fallidos.set(resumen.fallidos());
            if (resumen.masAntiguo() == null) {
                masAntiguoEpochMs.set(-1);
            } else {
                masAntiguoEpochMs.set(resumen.masAntiguo().toEpochMilli());
            }
        } catch (RuntimeException e) {
            log.warn("[AUDITORIA][OUTBOX] no se pudieron leer las métricas: {}", ErrorSaneado.de(e));
        }
    }

    /**
     * Eventos PENDIENTE y EN_PROCESO.
     *
     * @return la última lectura
     */
    public long pendientes() {
        return pendientes.get();
    }

    /**
     * Eventos FALLIDO.
     *
     * @return la última lectura
     */
    public long fallidos() {
        return fallidos.get();
    }

    /**
     * Segundos que lleva esperando el pendiente más antiguo de la última lectura; 0 si no hay pendientes.
     *
     * @return la edad
     */
    public double edadMasAntiguoSegundos() {
        final long creacion = masAntiguoEpochMs.get();
        if (creacion < 0) {
            return 0;
        }
        return Math.max(0, (reloj.millis() - creacion) / MILIS_POR_SEGUNDO);
    }
}
