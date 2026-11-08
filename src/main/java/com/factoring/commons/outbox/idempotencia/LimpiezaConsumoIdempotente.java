package com.factoring.commons.outbox.idempotencia;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.factoring.commons.auditoria.outbox.ErrorSaneado;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

/**
 * Borra en tandas, cada {@code consumo-idempotente.intervalo-limpieza} y en un hilo propio, las marcas más viejas que
 * la retención. El DELETE es idempotente y acotado: varias instancias a la vez no se pisan.
 */
@Slf4j
public class LimpiezaConsumoIdempotente implements SmartLifecycle {

    /** Marcas borradas por sentencia. */
    static final int TANDA = 1000;

    private static final long ESPERA_CIERRE_S = 10;

    private final ConsumoIdempotente consumo;
    private final Duration retencion;
    private final Duration intervalo;
    private final Clock reloj;
    private ScheduledExecutorService ejecutor;

    /**
     * @param consumo   consumo idempotente
     * @param retencion cuánto se conserva cada marca
     * @param intervalo pausa entre limpiezas
     * @param reloj     reloj de la retención
     */
    public LimpiezaConsumoIdempotente(final ConsumoIdempotente consumo, final Duration retencion,
            final Duration intervalo, final Clock reloj) {
        this.consumo = consumo;
        this.retencion = retencion;
        this.intervalo = intervalo;
        this.reloj = reloj;
    }

    /**
     * Una limpieza completa.
     *
     * @return marcas borradas
     */
    public int limpiar() {
        int total = 0;
        int borradas;
        do {
            borradas = consumo.limpiarAnteriores(reloj.instant().minus(retencion), TANDA);
            total += borradas;
        } while (borradas == TANDA);
        return total;
    }

    private void limpiarSeguro() {
        try {
            limpiar();
        } catch (RuntimeException e) {
            log.warn("[CONSUMO_IDEMPOTENTE] limpieza fallida: {}", ErrorSaneado.de(e));
        }
    }

    @Override
    public void start() {
        if (ejecutor != null) {
            return;
        }
        ejecutor = Executors.newSingleThreadScheduledExecutor(tarea -> {
            final Thread hilo = new Thread(tarea, "consumo-idempotente-limpieza");
            hilo.setDaemon(true);
            return hilo;
        });
        ejecutor.scheduleWithFixedDelay(this::limpiarSeguro, intervalo.toMillis(), intervalo.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (ejecutor == null) {
            return;
        }
        ejecutor.shutdown();
        try {
            if (!ejecutor.awaitTermination(ESPERA_CIERRE_S, TimeUnit.SECONDS)) {
                ejecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            ejecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        ejecutor = null;
    }

    @Override
    public boolean isRunning() {
        return ejecutor != null;
    }
}
