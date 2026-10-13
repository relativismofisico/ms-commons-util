package com.factoring.commons.auditoria;

import java.time.Instant;
import java.util.Map;

/**
 * Contrato ÚNICO del evento de auditoría de la plataforma (topic {@value AuditoriaConstantes#TOPIC_POR_DEFECTO}). Lo
 * publican los servicios productores y lo persiste ms-auditoria-util en {@code audit_log}: ningún productor redefine el
 * esquema, todos usan esta clase.
 *
 * <p>Se construye siempre con {@link #builder()}, que valida los campos obligatorios y que {@code detalle} no lleve
 * datos sensibles. Los campos que son catálogos ({@code accion}, {@code canal}, {@code entidad.tipo},
 * {@code actor.tipo}) viajan como String, no como enum: agregar un valor nuevo no rompe a un consumidor viejo.</p>
 *
 * @param eventId         identificador del evento; lo genera el productor UNA vez y se repite igual en cada reintento
 *                        (clave de idempotencia del consumidor)
 * @param schemaVersion   versión del esquema ({@value #SCHEMA_VERSION_ACTUAL})
 * @param occurredAt      cuándo ocurrió la acción (no cuándo se publicó)
 * @param servicioOrigen  servicio que ejecutó la acción ({@code spring.application.name})
 * @param accion          qué se hizo; ver {@link Acciones}
 * @param entidad         sobre qué se hizo
 * @param actor           quién lo hizo, tomado del JWT ya validado o explícito en procesos asincrónicos
 * @param canal           por dónde llegó la acción; ver {@link Canal}
 * @param ip              IP del cliente tomada en el servidor; {@code null} si no hubo request HTTP
 * @param requestId       id de correlación con los logs de la operación (MDC {@code requestId})
 * @param estadoAnterior  estado de la entidad antes de la acción, si aplica
 * @param estadoNuevo     estado de la entidad después de la acción, si aplica
 * @param detalle         datos propios de la acción (montos, motivo, observación); nunca secretos
 */
public record AuditoriaAccionEvent(
        String eventId,
        int schemaVersion,
        Instant occurredAt,
        String servicioOrigen,
        String accion,
        Entidad entidad,
        AuditoriaActor actor,
        String canal,
        String ip,
        String requestId,
        String estadoAnterior,
        String estadoNuevo,
        Map<String, Object> detalle) {

    /** Versión vigente del esquema. */
    public static final int SCHEMA_VERSION_ACTUAL = 1;

    /** Copia defensiva: {@code detalle} es inmutable una vez construido el evento. */
    public AuditoriaAccionEvent {
        if (detalle != null) {
            detalle = Map.copyOf(detalle);
        }
    }

    /**
     * Inicia la construcción de un evento.
     *
     * @return builder con {@code eventId}, {@code occurredAt} y {@code schemaVersion} ya inicializados
     */
    public static AuditoriaAccionEventBuilder builder() {
        return new AuditoriaAccionEventBuilder();
    }

    /**
     * La misma acción con el servicio de origen fijado. Lo usa el publicador cuando el productor no lo informó.
     *
     * @param servicio nombre del servicio
     * @return copia del evento con {@code servicioOrigen}
     */
    public AuditoriaAccionEvent conServicioOrigen(final String servicio) {
        return new AuditoriaAccionEvent(eventId, schemaVersion, occurredAt, servicio, accion, entidad, actor, canal, ip,
                requestId, estadoAnterior, estadoNuevo, detalle);
    }

    /**
     * Clave Kafka del evento: {@code <entidad.tipo>:<entidad.id>}, para que los eventos de una misma entidad caigan en
     * la misma partición y lleguen en orden.
     *
     * @return la clave
     */
    public String claveKafka() {
        return entidad.tipo() + ":" + entidad.id();
    }

    /**
     * Entidad sobre la que se ejecutó la acción.
     *
     * @param tipo tipo de entidad; ver {@link EntidadTipo}
     * @param id   identificador de la entidad en su servicio dueño
     */
    public record Entidad(String tipo, String id) {
    }
}
