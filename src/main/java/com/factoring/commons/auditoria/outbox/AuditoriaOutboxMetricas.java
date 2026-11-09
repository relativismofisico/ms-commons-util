package com.factoring.commons.auditoria.outbox;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.ToLongFunction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Gauges del outbox. Solo se registra si el servicio tiene Micrometer y un {@code MeterRegistry}.
 *
 * <p>Por tipo de evento, con tags {@code tipo} y {@code servicio}:</p>
 * <ul>
 *   <li>{@value #OUTBOX_PENDIENTES}: eventos PENDIENTE y EN_PROCESO;</li>
 *   <li>{@value #OUTBOX_EN_PROCESO}: eventos EN_PROCESO (reclamados por un relay, con lease);</li>
 *   <li>{@value #OUTBOX_FALLIDOS}: eventos FALLIDO (cada uno bloquea los posteriores de su clave: alertar si es mayor
 *       que 0);</li>
 *   <li>{@value #OUTBOX_EDAD}: segundos que lleva esperando el pendiente más antiguo;</li>
 *   <li>{@value #OUTBOX_DUPLICADOS} (contador, desde 0.6.1, solo tipos genéricos): registros con {@code eventId}
 *       propio descartados porque el almacén ya tenía ese {@code eventId}.</li>
 * </ul>
 *
 * <p>Alias de 0.5.x, con el total de todos los tipos y solo el tag {@code servicio}, durante 0.6.x:
 * {@value #PENDIENTES}, {@value #FALLIDOS} y {@value #EDAD}.</p>
 */
public class AuditoriaOutboxMetricas implements MeterBinder {

    /** Gauge de pendientes (alias 0.5.x, total). */
    public static final String PENDIENTES = "auditoria.outbox.pendientes";

    /** Gauge de fallidos (alias 0.5.x, total). */
    public static final String FALLIDOS = "auditoria.outbox.fallidos";

    /** Gauge de la edad del pendiente más antiguo (alias 0.5.x, total). */
    public static final String EDAD = "auditoria.outbox.edad_mas_antiguo_segundos";

    /** Gauge de pendientes por tipo. */
    public static final String OUTBOX_PENDIENTES = "outbox.pendientes";

    /** Gauge de eventos en proceso por tipo. */
    public static final String OUTBOX_EN_PROCESO = "outbox.en_proceso";

    /** Gauge de fallidos por tipo. */
    public static final String OUTBOX_FALLIDOS = "outbox.fallidos";

    /** Gauge de la edad del pendiente más antiguo por tipo. */
    public static final String OUTBOX_EDAD = "outbox.edad_mas_antiguo_segundos";

    /** Contador de registros descartados por {@code eventId} repetido, por tipo. */
    public static final String OUTBOX_DUPLICADOS = "outbox.duplicados";

    private static final String TAG_SERVICIO = "servicio";
    private static final String TAG_TIPO = "tipo";

    private final AuditoriaOutboxEstadisticas estadisticas;
    private final String servicio;
    private final Set<String> tipos;
    private final Set<String> genericos;
    private final ToLongFunction<String> duplicados;

    /**
     * Gauges solo para los eventos de auditoría.
     *
     * @param estadisticas estadísticas del outbox
     * @param servicio     {@code spring.application.name}, valor del tag {@code servicio}
     */
    public AuditoriaOutboxMetricas(final AuditoriaOutboxEstadisticas estadisticas, final String servicio) {
        this(estadisticas, servicio, Set.of());
    }

    /**
     * @param estadisticas estadísticas del outbox
     * @param servicio     {@code spring.application.name}, valor del tag {@code servicio}
     * @param tipos        tipos genéricos declarados (además de {@value AuditoriaOutboxEvento#TIPO_AUDITORIA})
     */
    public AuditoriaOutboxMetricas(final AuditoriaOutboxEstadisticas estadisticas, final String servicio,
            final Set<String> tipos) {
        this(estadisticas, servicio, tipos, null);
    }

    /**
     * @param estadisticas estadísticas del outbox
     * @param servicio     {@code spring.application.name}, valor del tag {@code servicio}
     * @param tipos        tipos genéricos declarados (además de {@value AuditoriaOutboxEvento#TIPO_AUDITORIA})
     * @param duplicados   registros descartados por {@code eventId} repetido, por tipo; {@code null} si no hay registro
     *                     de eventos genéricos (no se publica {@value #OUTBOX_DUPLICADOS})
     */
    public AuditoriaOutboxMetricas(final AuditoriaOutboxEstadisticas estadisticas, final String servicio,
            final Set<String> tipos, final ToLongFunction<String> duplicados) {
        this.estadisticas = estadisticas;
        this.genericos = Set.copyOf(tipos);
        this.duplicados = duplicados;
        this.servicio = servicio;
        final Set<String> todos = new LinkedHashSet<>();
        todos.add(AuditoriaOutboxEvento.TIPO_AUDITORIA);
        todos.addAll(tipos);
        this.tipos = Set.copyOf(todos);
    }

    @Override
    public void bindTo(final MeterRegistry registry) {
        estadisticas.activar();
        Gauge.builder(PENDIENTES, estadisticas, AuditoriaOutboxEstadisticas::pendientes)
                .description("Eventos del outbox sin publicar (PENDIENTE y EN_PROCESO), todos los tipos")
                .tag(TAG_SERVICIO, servicio).register(registry);
        Gauge.builder(FALLIDOS, estadisticas, AuditoriaOutboxEstadisticas::fallidos)
                .description("Eventos del outbox FALLIDO, todos los tipos; cada uno bloquea los posteriores de su clave")
                .tag(TAG_SERVICIO, servicio).register(registry);
        Gauge.builder(EDAD, estadisticas, AuditoriaOutboxEstadisticas::edadMasAntiguoSegundos)
                .description("Segundos que lleva esperando el evento pendiente más antiguo del outbox")
                .baseUnit("seconds").tag(TAG_SERVICIO, servicio).register(registry);
        for (final String tipo : tipos) {
            Gauge.builder(OUTBOX_PENDIENTES, estadisticas, e -> e.pendientes(tipo))
                    .description("Eventos del outbox sin publicar (PENDIENTE y EN_PROCESO)")
                    .tag(TAG_TIPO, tipo).tag(TAG_SERVICIO, servicio).register(registry);
            Gauge.builder(OUTBOX_EN_PROCESO, estadisticas, e -> e.enProceso(tipo))
                    .description("Eventos del outbox reclamados por un relay (EN_PROCESO)")
                    .tag(TAG_TIPO, tipo).tag(TAG_SERVICIO, servicio).register(registry);
            Gauge.builder(OUTBOX_FALLIDOS, estadisticas, e -> e.fallidos(tipo))
                    .description("Eventos del outbox FALLIDO; cada uno bloquea los posteriores de su clave")
                    .tag(TAG_TIPO, tipo).tag(TAG_SERVICIO, servicio).register(registry);
            Gauge.builder(OUTBOX_EDAD, estadisticas, e -> e.edadMasAntiguoSegundos(tipo))
                    .description("Segundos que lleva esperando el evento pendiente más antiguo")
                    .baseUnit("seconds").tag(TAG_TIPO, tipo).tag(TAG_SERVICIO, servicio).register(registry);
        }
        if (duplicados != null) {
            for (final String tipo : genericos) {
                FunctionCounter.builder(OUTBOX_DUPLICADOS, duplicados, d -> d.applyAsLong(tipo))
                        .description("Registros con eventId propio descartados porque ese eventId ya estaba en el outbox")
                        .tag(TAG_TIPO, tipo).tag(TAG_SERVICIO, servicio).register(registry);
            }
        }
    }
}
