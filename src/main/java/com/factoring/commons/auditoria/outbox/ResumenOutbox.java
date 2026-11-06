package com.factoring.commons.auditoria.outbox;

import java.time.Instant;

/**
 * Estado del outbox para las métricas.
 *
 * @param pendientes eventos sin enviar ni fallar (PENDIENTE y EN_PROCESO)
 * @param fallidos   eventos FALLIDO (cada uno bloquea los posteriores de su clave)
 * @param masAntiguo fecha de creación del pendiente más antiguo; {@code null} si no hay pendientes
 */
public record ResumenOutbox(long pendientes, long fallidos, Instant masAntiguo) {

    /** Outbox vacío. */
    public static final ResumenOutbox VACIO = new ResumenOutbox(0, 0, null);
}
