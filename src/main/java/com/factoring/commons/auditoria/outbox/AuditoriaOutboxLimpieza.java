package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import java.time.Instant;

import lombok.extern.slf4j.Slf4j;

/**
 * Borra los eventos ENVIADO más viejos que {@code auditoria.outbox.retencion}, en tandas, bajo
 * {@link BloqueoLimpieza}. Los PENDIENTE, EN_PROCESO y FALLIDO no se tocan nunca.
 */
@Slf4j
public class AuditoriaOutboxLimpieza {

    /** Eventos borrados por sentencia, para no retener bloqueos largos sobre la tabla o la colección. */
    static final int TANDA = 1000;

    private final AuditoriaOutboxPort outbox;
    private final AuditoriaOutboxProperties propiedades;
    private final BloqueoLimpieza bloqueo;
    private final String nombreBloqueo;
    private final Clock reloj;

    /**
     * @param outbox         almacén del outbox
     * @param propiedades    {@code auditoria.outbox.*}
     * @param bloqueo        bloqueo distribuido
     * @param servicioOrigen {@code spring.application.name}, para el nombre del bloqueo
     * @param reloj          reloj de la retención
     */
    public AuditoriaOutboxLimpieza(final AuditoriaOutboxPort outbox, final AuditoriaOutboxProperties propiedades,
            final BloqueoLimpieza bloqueo, final String servicioOrigen, final Clock reloj) {
        this.outbox = outbox;
        this.propiedades = propiedades;
        this.bloqueo = bloqueo;
        this.nombreBloqueo = "auditoria-outbox-limpieza-" + servicioOrigen;
        this.reloj = reloj;
    }

    /**
     * Una limpieza completa.
     *
     * @return eventos borrados (0 si otra instancia tenía el bloqueo)
     */
    public int limpiar() {
        final int[] borrados = {0};
        bloqueo.ejecutar(nombreBloqueo, () -> borrados[0] = borrarEnTandas());
        return borrados[0];
    }

    /** {@link #limpiar()} sin dejar escapar excepciones (para el planificador). */
    public void limpiarSeguro() {
        try {
            final int borrados = limpiar();
            if (borrados > 0) {
                log.info("[AUDITORIA][OUTBOX] limpieza: {} eventos enviados borrados", borrados);
            }
        } catch (RuntimeException e) {
            log.error("[AUDITORIA][OUTBOX] limpieza fallida: {}", ErrorSaneado.de(e));
        }
    }

    private int borrarEnTandas() {
        final Instant limite = reloj.instant().minus(propiedades.getRetencion());
        int total = 0;
        int tanda;
        do {
            tanda = outbox.limpiarEnviadosAnteriores(limite, TANDA);
            total += tanda;
        } while (tanda >= TANDA);
        return total;
    }
}
