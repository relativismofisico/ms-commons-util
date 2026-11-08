package com.factoring.commons.auditoria.outbox;

import java.time.Instant;

/**
 * Un evento guardado en el outbox: uno de auditoría (0.5.x, {@code tipo == null}) o uno genérico (0.6.0, con
 * {@code tipo} y el sobre {@code EventoOutbox} como payload).
 *
 * @param id             identificador de la fila en el almacén ({@code null} antes de guardarse)
 * @param eventId        el {@code eventId} del evento, igual en cada reintento
 * @param claveKafka     clave Kafka del evento; ordena los eventos de una misma clave
 * @param topic          topic donde se publica
 * @param payload        el evento serializado: {@code AuditoriaJson} si {@code tipo} es nulo; si no, el sobre
 * @param estado         estado actual
 * @param intentos       publicaciones fallidas (incluye leases vencidos)
 * @param proximoIntento desde cuándo se puede volver a tomar
 * @param leaseHasta     hasta cuándo lo tiene reclamado un relay ({@code null} fuera de {@link EstadoOutbox#EN_PROCESO})
 * @param fechaCreacion  cuándo se guardó
 * @param fechaEnvio     cuándo lo confirmó Kafka
 * @param ultimoError    causa del último fallo, saneada (solo tipos de excepción o un código fijo, nunca el payload)
 * @param tipo           tipo declarado del evento genérico; {@code null} en los eventos de auditoría
 */
public record AuditoriaOutboxEvento(
        String id,
        String eventId,
        String claveKafka,
        String topic,
        String payload,
        EstadoOutbox estado,
        int intentos,
        Instant proximoIntento,
        Instant leaseHasta,
        Instant fechaCreacion,
        Instant fechaEnvio,
        String ultimoError,
        String tipo) {

    /** Tipo con el que se cuentan en las métricas los eventos de auditoría, que no guardan {@code tipo}. */
    public static final String TIPO_AUDITORIA = "auditoria.accion";

    /**
     * Evento de auditoría (sin {@code tipo}), con la forma de 0.5.x.
     *
     * @param id             identificador de la fila
     * @param eventId        {@code eventId} del evento
     * @param claveKafka     clave Kafka
     * @param topic          topic de destino
     * @param payload        el evento serializado
     * @param estado         estado actual
     * @param intentos       publicaciones fallidas
     * @param proximoIntento desde cuándo se puede volver a tomar
     * @param leaseHasta     fin del lease
     * @param fechaCreacion  cuándo se guardó
     * @param fechaEnvio     cuándo lo confirmó Kafka
     * @param ultimoError    causa saneada del último fallo
     */
    @SuppressWarnings({"checkstyle:ParameterNumber", "PMD.ExcessiveParameterList"})
    public AuditoriaOutboxEvento(final String id, final String eventId, final String claveKafka, final String topic,
            final String payload, final EstadoOutbox estado, final int intentos, final Instant proximoIntento,
            final Instant leaseHasta, final Instant fechaCreacion, final Instant fechaEnvio, final String ultimoError) {
        this(id, eventId, claveKafka, topic, payload, estado, intentos, proximoIntento, leaseHasta, fechaCreacion,
                fechaEnvio, ultimoError, null);
    }

    /**
     * Un evento nuevo, pendiente y listo para tomarse.
     *
     * @param eventId    {@code eventId} del evento
     * @param claveKafka clave Kafka del evento
     * @param topic      topic de destino
     * @param payload    el evento serializado
     * @param ahora      fecha de creación y de primer intento
     * @return el evento sin {@code id} ni {@code tipo}
     */
    public static AuditoriaOutboxEvento nuevo(final String eventId, final String claveKafka, final String topic,
            final String payload, final Instant ahora) {
        return new AuditoriaOutboxEvento(null, eventId, claveKafka, topic, payload, EstadoOutbox.PENDIENTE, 0, ahora,
                null, ahora, null, null, null);
    }

    /**
     * El mismo evento con su tipo declarado (evento genérico).
     *
     * @param tipoDeclarado tipo del evento
     * @return copia con {@code tipo}
     */
    public AuditoriaOutboxEvento conTipo(final String tipoDeclarado) {
        return new AuditoriaOutboxEvento(id, eventId, claveKafka, topic, payload, estado, intentos, proximoIntento,
                leaseHasta, fechaCreacion, fechaEnvio, ultimoError, tipoDeclarado);
    }

    /**
     * El mismo evento reclamado por un relay.
     *
     * @param hasta fin del lease
     * @return copia en {@link EstadoOutbox#EN_PROCESO}
     */
    public AuditoriaOutboxEvento enProceso(final Instant hasta) {
        return new AuditoriaOutboxEvento(id, eventId, claveKafka, topic, payload, EstadoOutbox.EN_PROCESO, intentos,
                proximoIntento, hasta, fechaCreacion, fechaEnvio, ultimoError, tipo);
    }

    /**
     * Tipo para las métricas: el declarado, o {@value #TIPO_AUDITORIA} en los eventos de auditoría.
     *
     * @return el tipo
     */
    public String tipoMetricas() {
        if (tipo == null) {
            return TIPO_AUDITORIA;
        }
        return tipo;
    }
}
