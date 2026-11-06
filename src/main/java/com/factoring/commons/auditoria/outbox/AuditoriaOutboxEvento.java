package com.factoring.commons.auditoria.outbox;

import java.time.Instant;

/**
 * Un evento de auditoría guardado en el outbox.
 *
 * @param id             identificador de la fila en el almacén ({@code null} antes de guardarse)
 * @param eventId        el {@code eventId} del {@code AuditoriaAccionEvent}, igual en cada reintento
 * @param claveKafka     clave Kafka del evento ({@code <entidad.tipo>:<entidad.id>}); ordena los eventos de una entidad
 * @param topic          topic donde se publica
 * @param payload        el evento serializado con {@code AuditoriaJson}
 * @param estado         estado actual
 * @param intentos       publicaciones fallidas (incluye leases vencidos)
 * @param proximoIntento desde cuándo se puede volver a tomar
 * @param leaseHasta     hasta cuándo lo tiene reclamado un relay ({@code null} fuera de {@link EstadoOutbox#EN_PROCESO})
 * @param fechaCreacion  cuándo se guardó
 * @param fechaEnvio     cuándo lo confirmó Kafka
 * @param ultimoError    causa del último fallo, saneada (solo tipos de excepción o un código fijo, nunca el payload)
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
        String ultimoError) {

    /**
     * Un evento nuevo, pendiente y listo para tomarse.
     *
     * @param eventId    {@code eventId} del evento de auditoría
     * @param claveKafka clave Kafka del evento
     * @param topic      topic de destino
     * @param payload    el evento serializado
     * @param ahora      fecha de creación y de primer intento
     * @return el evento sin {@code id}
     */
    public static AuditoriaOutboxEvento nuevo(final String eventId, final String claveKafka, final String topic,
            final String payload, final Instant ahora) {
        return new AuditoriaOutboxEvento(null, eventId, claveKafka, topic, payload, EstadoOutbox.PENDIENTE, 0, ahora,
                null, ahora, null, null);
    }

    /**
     * El mismo evento reclamado por un relay.
     *
     * @param hasta fin del lease
     * @return copia en {@link EstadoOutbox#EN_PROCESO}
     */
    public AuditoriaOutboxEvento enProceso(final Instant hasta) {
        return new AuditoriaOutboxEvento(id, eventId, claveKafka, topic, payload, EstadoOutbox.EN_PROCESO, intentos,
                proximoIntento, hasta, fechaCreacion, fechaEnvio, ultimoError);
    }
}
