package com.factoring.commons.outbox;

/** El broker no confirmó la publicación de un evento del outbox; el relay lo deja para reintentarlo. */
public class OutboxNoPublicadoException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * @param eventId id del evento no publicado
     * @param causa   el fallo de Kafka
     */
    public OutboxNoPublicadoException(final String eventId, final Throwable causa) {
        super("Evento de outbox no publicado: " + eventId, causa);
    }
}
