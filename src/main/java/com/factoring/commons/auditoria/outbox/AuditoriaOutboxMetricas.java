package com.factoring.commons.auditoria.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Gauges del outbox, con tag {@code servicio}. Solo se registra si el servicio tiene Micrometer y un
 * {@code MeterRegistry}:
 * <ul>
 *   <li>{@value #PENDIENTES}: eventos PENDIENTE y EN_PROCESO;</li>
 *   <li>{@value #FALLIDOS}: eventos FALLIDO (cada uno bloquea los posteriores de su clave: alertar si es mayor que
 *       0);</li>
 *   <li>{@value #EDAD}: segundos que lleva esperando el pendiente más antiguo.</li>
 * </ul>
 */
public class AuditoriaOutboxMetricas implements MeterBinder {

    /** Gauge de pendientes. */
    public static final String PENDIENTES = "auditoria.outbox.pendientes";

    /** Gauge de fallidos. */
    public static final String FALLIDOS = "auditoria.outbox.fallidos";

    /** Gauge de la edad del pendiente más antiguo. */
    public static final String EDAD = "auditoria.outbox.edad_mas_antiguo_segundos";

    private final AuditoriaOutboxEstadisticas estadisticas;
    private final String servicio;

    /**
     * @param estadisticas estadísticas del outbox
     * @param servicio     {@code spring.application.name}, valor del tag {@code servicio}
     */
    public AuditoriaOutboxMetricas(final AuditoriaOutboxEstadisticas estadisticas, final String servicio) {
        this.estadisticas = estadisticas;
        this.servicio = servicio;
    }

    @Override
    public void bindTo(final MeterRegistry registry) {
        estadisticas.activar();
        Gauge.builder(PENDIENTES, estadisticas, AuditoriaOutboxEstadisticas::pendientes)
                .description("Eventos de auditoría sin publicar (PENDIENTE y EN_PROCESO)")
                .tag("servicio", servicio).register(registry);
        Gauge.builder(FALLIDOS, estadisticas, AuditoriaOutboxEstadisticas::fallidos)
                .description("Eventos de auditoría FALLIDO; cada uno bloquea los posteriores de su clave")
                .tag("servicio", servicio).register(registry);
        Gauge.builder(EDAD, estadisticas, AuditoriaOutboxEstadisticas::edadMasAntiguoSegundos)
                .description("Segundos que lleva esperando el evento de auditoría pendiente más antiguo")
                .baseUnit("seconds").tag("servicio", servicio).register(registry);
    }
}
