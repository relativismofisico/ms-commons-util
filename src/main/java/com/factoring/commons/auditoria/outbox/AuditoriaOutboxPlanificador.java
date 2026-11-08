package com.factoring.commons.auditoria.outbox;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.context.SmartLifecycle;

/**
 * Corre el relay cada {@code auditoria.outbox.intervalo} y la limpieza cada {@code auditoria.outbox.intervalo-limpieza}
 * en un hilo propio, y el refresco de las métricas cada {@code auditoria.outbox.intervalo-metricas} en OTRO hilo: un
 * relay esperando un ack (hasta {@code timeout-ack} por evento) no congela las métricas, que es justo cuando más
 * sirven. No depende de que el servicio active {@code @EnableScheduling}.
 */
public class AuditoriaOutboxPlanificador implements SmartLifecycle {

    private static final long ESPERA_CIERRE_S = 10;

    private final OutboxAuditoriaRelay relay;
    private final AuditoriaOutboxLimpieza limpieza;
    private final AuditoriaOutboxProperties propiedades;
    private final AuditoriaOutboxEstadisticas estadisticas;
    private ScheduledExecutorService ejecutor;
    private ScheduledExecutorService ejecutorMetricas;

    /**
     * @param relay       relay del outbox
     * @param limpieza    limpieza del outbox
     * @param propiedades {@code auditoria.outbox.*}
     */
    public AuditoriaOutboxPlanificador(final OutboxAuditoriaRelay relay, final AuditoriaOutboxLimpieza limpieza,
            final AuditoriaOutboxProperties propiedades) {
        this(relay, limpieza, propiedades, null);
    }

    /**
     * @param relay        relay del outbox
     * @param limpieza     limpieza del outbox
     * @param propiedades  {@code auditoria.outbox.*}
     * @param estadisticas estadísticas para las métricas ({@code null}: sin métricas)
     */
    public AuditoriaOutboxPlanificador(final OutboxAuditoriaRelay relay, final AuditoriaOutboxLimpieza limpieza,
            final AuditoriaOutboxProperties propiedades, final AuditoriaOutboxEstadisticas estadisticas) {
        this.relay = relay;
        this.limpieza = limpieza;
        this.propiedades = propiedades;
        this.estadisticas = estadisticas;
    }

    @Override
    public void start() {
        if (ejecutor != null) {
            return;
        }
        ejecutor = Executors.newSingleThreadScheduledExecutor(tarea -> hilo(tarea, "auditoria-outbox"));
        final long intervalo = propiedades.getIntervalo().toMillis();
        ejecutor.scheduleWithFixedDelay(relay::ejecutarCicloSeguro, intervalo, intervalo, TimeUnit.MILLISECONDS);
        final long intervaloLimpieza = propiedades.getIntervaloLimpieza().toMillis();
        ejecutor.scheduleWithFixedDelay(limpieza::limpiarSeguro, intervaloLimpieza, intervaloLimpieza,
                TimeUnit.MILLISECONDS);
        if (estadisticas != null) {
            ejecutorMetricas = Executors.newSingleThreadScheduledExecutor(tarea -> hilo(tarea, "outbox-metricas"));
            ejecutorMetricas.scheduleWithFixedDelay(estadisticas::actualizarSeguro, 0,
                    propiedades.getIntervaloMetricas().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static Thread hilo(final Runnable tarea, final String nombre) {
        final Thread hilo = new Thread(tarea, nombre);
        hilo.setDaemon(true);
        return hilo;
    }

    @Override
    public void stop() {
        if (ejecutor == null) {
            return;
        }
        detener(ejecutorMetricas);
        detener(ejecutor);
        ejecutor = null;
        ejecutorMetricas = null;
    }

    private static void detener(final ScheduledExecutorService servicio) {
        if (servicio == null) {
            return;
        }
        servicio.shutdown();
        try {
            if (!servicio.awaitTermination(ESPERA_CIERRE_S, TimeUnit.SECONDS)) {
                servicio.shutdownNow();
            }
        } catch (InterruptedException e) {
            servicio.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return ejecutor != null;
    }
}
