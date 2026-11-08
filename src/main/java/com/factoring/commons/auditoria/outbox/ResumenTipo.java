package com.factoring.commons.auditoria.outbox;

import java.time.Instant;

/**
 * Estado del outbox de un tipo de evento, para las métricas por tipo.
 *
 * @param pendientes eventos sin enviar ni fallar (PENDIENTE y EN_PROCESO)
 * @param enProceso  eventos EN_PROCESO (reclamados por un relay, con lease); ya incluidos en {@code pendientes}
 * @param fallidos   eventos FALLIDO (cada uno bloquea los posteriores de su clave)
 * @param masAntiguo fecha de creación del pendiente más antiguo; {@code null} si no hay pendientes
 */
public record ResumenTipo(long pendientes, long enProceso, long fallidos, Instant masAntiguo) {

    /** Tipo sin eventos por enviar. */
    public static final ResumenTipo VACIO = new ResumenTipo(0, 0, 0, null);

    /**
     * El resumen total de un almacén sin desglose por tipo (sin conteo de EN_PROCESO).
     *
     * @param resumen resumen total
     * @return el mismo resumen como un único tipo
     */
    public static ResumenTipo de(final ResumenOutbox resumen) {
        return new ResumenTipo(resumen.pendientes(), 0, resumen.fallidos(), resumen.masAntiguo());
    }
}
