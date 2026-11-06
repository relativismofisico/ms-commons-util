package com.factoring.commons.auditoria;

/**
 * Registra una acción auditada desde un caso de uso. Con {@code auditoria.outbox.enabled=true} la autoconfiguración
 * registra una implementación respaldada por el outbox ({@code OutboxAuditoriaPort}): el evento se guarda en el mismo
 * almacén y en la misma transacción que el cambio de negocio, y un relay lo publica después en Kafka.
 *
 * <p>El adaptador de auditoría de cada servicio (el que implementa su propio puerto de salida) delega en este bean
 * cuando existe.</p>
 */
public interface AuditoriaPort {

    /**
     * Registra el evento. Si no se puede registrar, lanza la excepción: el caso de uso no debe confirmar sin su
     * evento.
     *
     * @param evento la acción, armada con {@link AuditoriaAccionEvent#builder()}
     */
    void registrar(AuditoriaAccionEvent evento);
}
