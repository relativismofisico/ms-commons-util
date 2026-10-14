package com.factoring.commons.auditoria;

/**
 * Kafka no confirmó un evento de auditoría publicado con {@link AuditoriaPublisher#publicarConfirmado}. El llamador (el
 * relay de un outbox) deja el evento pendiente y lo reintenta.
 */
public class AuditoriaNoPublicadaException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * @param eventId id del evento que no se publicó
     * @param causa   el error de Kafka
     */
    public AuditoriaNoPublicadaException(final String eventId, final Throwable causa) {
        super("Kafka no confirmó el evento de auditoría " + eventId, causa);
    }
}
