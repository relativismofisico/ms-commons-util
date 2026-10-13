package com.factoring.commons.auditoria;

/**
 * Publica eventos de auditoría. La implementación por defecto ({@link KafkaAuditoriaPublisher}) la registra la
 * autoconfiguración en todo servicio que tenga Kafka.
 *
 * <p>Se llama DESPUÉS de que el cambio de negocio quedó guardado: nunca se audita algo que no ocurrió. Un fallo al
 * publicar no revierte la acción (ya está guardada): se registra en el log con el evento completo para poder
 * reenviarlo.</p>
 */
public interface AuditoriaPublisher {

    /**
     * Publica el evento. No lanza excepción si Kafka falla: lo registra en el log.
     *
     * @param evento evento construido con {@link AuditoriaAccionEvent#builder()}
     */
    void publicar(AuditoriaAccionEvent evento);
}
