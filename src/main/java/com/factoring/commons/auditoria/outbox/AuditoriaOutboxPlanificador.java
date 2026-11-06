package com.factoring.commons.auditoria.outbox;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.context.SmartLifecycle;

/**
 * Corre el relay cada {@code auditoria.outbox.intervalo}, la limpieza cada {@code auditoria.outbox.intervalo-limpieza}
 * y el refresco de las métricas cada {@code auditoria.outbox.intervalo-metricas}, en un hilo propio, sin depender de
 * que el servicio active {@code @EnableScheduling}.
 */
public class AuditoriaOutboxPlanificador implements SmartLifecycle {

    private static final long ESPERA_CIERRE_S = 10;

    private final OutboxAuditoriaRelay relay;
    private final AuditoriaOutboxLimpieza limpieza;
    private final AuditoriaOutboxProperties propiedades;
    private final AuditoriaOutboxEstadisticas estadisticas;
    private ScheduledExecutorService ejecutor;

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
        ejecutor = Executors.newSingleThreadScheduledExecutor(tarea -> {
            final Thread hilo = new Thread(tarea, "auditoria-outbox");
            hilo.setDaemon(true);
            return hilo;
        });
        final long intervalo = propiedades.getIntervalo().toMillis();
        ejecutor.scheduleWithFixedDelay(relay::ejecutarCicloSeguro, intervalo, intervalo, TimeUnit.MILLISECONDS);
        final long intervaloLimpieza = propiedades.getIntervaloLimpieza().toMillis();
        ejecutor.scheduleWithFixedDelay(limpieza::limpiarSeguro, intervaloLimpieza, intervaloLimpieza,
                TimeUnit.MILLISECONDS);
        if (estadisticas != null) {
            ejecutor.scheduleWithFixedDelay(estadisticas::actualizarSeguro, 0,
                    propiedades.getIntervaloMetricas().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop() {
        if (ejecutor == null) {
            return;
        }
        ejecutor.shutdown();
        try {
            if (!ejecutor.awaitTermination(ESPERA_CIERRE_S, TimeUnit.SECONDS)) {
                ejecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            ejecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        ejecutor = null;
    }

    @Override
    public boolean isRunning() {
        return ejecutor != null;
    }
}
