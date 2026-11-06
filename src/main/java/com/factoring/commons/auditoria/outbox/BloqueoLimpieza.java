package com.factoring.commons.auditoria.outbox;

/**
 * Bloqueo distribuido de la limpieza del outbox. El relay no lo necesita (el almacén reparte los eventos sin
 * duplicarlos); la limpieza sí, para que varias instancias no borren lo mismo a la vez. Si el servicio tiene un
 * {@code LockProvider} de ShedLock se usa ese; si no, la limpieza corre sin bloqueo (es idempotente: solo se repite
 * el trabajo).
 */
@FunctionalInterface
public interface BloqueoLimpieza {

    /** Sin bloqueo: ejecuta la tarea en el momento. */
    BloqueoLimpieza SIN_BLOQUEO = (nombre, tarea) -> tarea.run();

    /**
     * Ejecuta la tarea si obtiene el bloqueo; si otra instancia lo tiene, no hace nada.
     *
     * @param nombre nombre del bloqueo
     * @param tarea  la limpieza
     */
    void ejecutar(String nombre, Runnable tarea);
}
