package com.factoring.commons.outbox;

import java.util.Optional;

/**
 * Registra eventos en el outbox del servicio, dentro de su transacción de negocio: el evento se confirma o se revierte
 * junto con el cambio. El relay lo publica después, con reintentos, en orden por clave.
 */
public interface OutboxPort {

    /**
     * Registra un evento de un tipo declarado. Genera el {@code eventId} (UUID), toma la hora, el {@code requestId}
     * del MDC y la clave de la función del tipo, y guarda el sobre en el almacén participando en la transacción
     * activa. Si la escritura falla, la excepción se propaga.
     *
     * @param tipo    nombre del tipo declarado en {@link TiposDeEvento}
     * @param payload datos del evento (de la clase declarada para el tipo)
     * @param <T>     tipo del payload
     * @return el sobre guardado
     * @throws TipoDeEventoNoDeclaradoException si el tipo no está declarado
     * @throws IllegalArgumentException          si el payload no es de la clase declarada o su clave sale vacía
     * @throws OutboxSoloDrenarException         si el outbox está en modo "solo drenar"
     */
    <T> EventoOutbox<T> registrar(String tipo, T payload);

    /**
     * Como {@link #registrar(String, Object)}, con el {@code eventId} que el productor ya tiene (por ejemplo el id del
     * agregado que origina el evento). Ese valor es el {@code eventId} del sobre guardado, el de la fila o documento y
     * el de la cabecera del mensaje.
     *
     * <p>Si el almacén ya tiene un evento con ese {@code eventId}, no escribe un segundo evento y no lanza: la
     * transacción de negocio sigue su curso. Devuelve vacío, lo cuenta en {@code outbox.duplicados} y deja un WARN.
     * El evento que ya estaba no cambia.</p>
     *
     * <p>Desde 0.6.1. La implementación por defecto lanza {@link UnsupportedOperationException}; la de la librería
     * ({@link OutboxRegistro}) la implementa.</p>
     *
     * @param tipo    nombre del tipo declarado en {@link TiposDeEvento}
     * @param eventId id del evento: no vacío y de hasta {@value EventoOutbox#LONGITUD_MAXIMA_EVENT_ID} caracteres
     * @param payload datos del evento (de la clase declarada para el tipo)
     * @param <T>     tipo del payload
     * @return el sobre guardado, o vacío si ya había un evento con ese {@code eventId}
     * @throws TipoDeEventoNoDeclaradoException si el tipo no está declarado
     * @throws IllegalArgumentException          si el {@code eventId} es vacío o largo, el payload no es de la clase
     *                                           declarada o su clave sale vacía
     * @throws OutboxSoloDrenarException         si el outbox está en modo "solo drenar"
     */
    default <T> Optional<EventoOutbox<T>> registrar(String tipo, String eventId, T payload) {
        throw new UnsupportedOperationException("Este OutboxPort no admite eventId propio");
    }
}
