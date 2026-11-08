package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

/**
 * Último resumen del almacén, total y por tipo, para las métricas. Lo refresca el planificador cada
 * {@code auditoria.outbox.intervalo-metricas} en un hilo propio, separado del relay (un relay esperando un ack no lo
 * congela), y solo si alguien lo usa (las gauges de Micrometer lo activan): así un servicio sin Micrometer no consulta
 * el almacén de más. Leer las gauges nunca consulta la base.
 */
@Slf4j
public class AuditoriaOutboxEstadisticas {

    private static final double MILIS_POR_SEGUNDO = 1000.0;

    private final AuditoriaOutboxPort outbox;
    private final Clock reloj;
    private final AtomicBoolean activa = new AtomicBoolean();
    private final AtomicReference<Lectura> ultima = new AtomicReference<>(
            new Lectura(ResumenOutbox.VACIO, 0, Map.of()));

    /** Una lectura completa del almacén; se reemplaza entera para que las gauges nunca mezclen dos lecturas. */
    private record Lectura(ResumenOutbox total, long enProceso, Map<String, ResumenTipo> porTipo) {
    }

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
            final ResumenOutbox total = outbox.resumen();
            Map<String, ResumenTipo> porTipo = outbox.resumenPorTipo();
            if (porTipo == null) {
                porTipo = Map.of();
            }
            long enProceso = 0;
            for (final ResumenTipo resumen : porTipo.values()) {
                enProceso += resumen.enProceso();
            }
            ultima.set(new Lectura(total, enProceso, Map.copyOf(porTipo)));
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
        return ultima.get().total().pendientes();
    }

    /**
     * Eventos FALLIDO.
     *
     * @return la última lectura
     */
    public long fallidos() {
        return ultima.get().total().fallidos();
    }

    /**
     * Eventos EN_PROCESO (reclamados por un relay).
     *
     * @return la última lectura
     */
    public long enProceso() {
        return ultima.get().enProceso();
    }

    /**
     * Segundos que lleva esperando el pendiente más antiguo de la última lectura; 0 si no hay pendientes.
     *
     * @return la edad
     */
    public double edadMasAntiguoSegundos() {
        return edad(ultima.get().total().masAntiguo());
    }

    /**
     * Eventos PENDIENTE y EN_PROCESO de un tipo.
     *
     * @param tipo tipo del evento
     * @return la última lectura (0 si no tiene)
     */
    public long pendientes(final String tipo) {
        return delTipo(tipo).pendientes();
    }

    /**
     * Eventos FALLIDO de un tipo.
     *
     * @param tipo tipo del evento
     * @return la última lectura (0 si no tiene)
     */
    public long fallidos(final String tipo) {
        return delTipo(tipo).fallidos();
    }

    /**
     * Eventos EN_PROCESO de un tipo.
     *
     * @param tipo tipo del evento
     * @return la última lectura (0 si no tiene)
     */
    public long enProceso(final String tipo) {
        return delTipo(tipo).enProceso();
    }

    /**
     * Segundos que lleva esperando el pendiente más antiguo de un tipo.
     *
     * @param tipo tipo del evento
     * @return la edad (0 si no tiene pendientes)
     */
    public double edadMasAntiguoSegundos(final String tipo) {
        return edad(delTipo(tipo).masAntiguo());
    }

    private ResumenTipo delTipo(final String tipo) {
        return ultima.get().porTipo().getOrDefault(tipo, ResumenTipo.VACIO);
    }

    private double edad(final Instant masAntiguo) {
        if (masAntiguo == null) {
            return 0;
        }
        return Math.max(0, (reloj.millis() - masAntiguo.toEpochMilli()) / MILIS_POR_SEGUNDO);
    }
}
