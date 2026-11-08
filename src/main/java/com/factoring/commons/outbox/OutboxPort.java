package com.factoring.commons.outbox;

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
}
