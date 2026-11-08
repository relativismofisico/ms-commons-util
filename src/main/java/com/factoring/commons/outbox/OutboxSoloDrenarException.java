package com.factoring.commons.outbox;

/**
 * El outbox está en modo "solo drenar" ({@code auditoria.outbox.solo-drenar=true}): el relay sigue publicando lo
 * pendiente, pero no acepta eventos nuevos. Quien registra decide su camino alternativo.
 */
public class OutboxSoloDrenarException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * @param tipo tipo del evento rechazado
     */
    public OutboxSoloDrenarException(final String tipo) {
        super("El outbox está en modo solo drenar y no acepta eventos nuevos (" + tipo + ")");
    }
}
