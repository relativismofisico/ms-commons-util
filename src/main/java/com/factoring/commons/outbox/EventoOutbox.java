package com.factoring.commons.outbox;

import java.time.Instant;

/**
 * Sobre de un evento del outbox genérico. Es el valor del mensaje Kafka (JSON de {@link OutboxJson}); además viajan
 * las cabeceras {@value #CABECERA_EVENT_ID}, {@value #CABECERA_TIPO}, {@value #CABECERA_VERSION} y
 * {@value #CABECERA_SERVICIO_ORIGEN}, para que un consumidor pueda deduplicar y enrutar sin leer el payload.
 *
 * @param eventId        UUID que genera el productor al registrar el evento, dentro de su transacción, o el id propio
 *                       que pasa el productor (desde 0.6.1); es la clave de idempotencia de punta a punta (el relay y
 *                       los reintentos no lo cambian)
 * @param tipo           nombre lógico estable del evento, declarado en {@link TiposDeEvento}
 * @param version        versión del esquema del payload
 * @param servicioOrigen servicio que lo produjo ({@code spring.application.name})
 * @param ocurridoEn     instante UTC en que se registró
 * @param requestId      id de correlación con los logs (MDC {@code requestId}); puede ser nulo
 * @param clave          clave Kafka y de orden: la calcula la función declarada del tipo
 * @param payload        datos del evento
 * @param <T>            tipo del payload
 */
public record EventoOutbox<T>(
        String eventId,
        String tipo,
        int version,
        String servicioOrigen,
        Instant ocurridoEn,
        String requestId,
        String clave,
        T payload) {

    /** Longitud máxima de un {@code eventId} propio: la de la columna {@code event_id} del esquema JDBC. */
    public static final int LONGITUD_MAXIMA_EVENT_ID = 64;

    /** Cabecera Kafka con el {@code eventId}. */
    public static final String CABECERA_EVENT_ID = "eventId";

    /** Cabecera Kafka con el {@code tipo}. */
    public static final String CABECERA_TIPO = "tipo";

    /** Cabecera Kafka con la {@code version}. */
    public static final String CABECERA_VERSION = "version";

    /** Cabecera Kafka con el {@code servicioOrigen}. */
    public static final String CABECERA_SERVICIO_ORIGEN = "servicioOrigen";
}
