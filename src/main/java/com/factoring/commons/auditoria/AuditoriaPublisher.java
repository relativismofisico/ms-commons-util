package com.factoring.commons.auditoria;

import java.time.Duration;

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

    /**
     * Publica el evento y espera la confirmación del broker. Para el relay de un outbox: si Kafka falla lanza
     * {@link AuditoriaNoPublicadaException} y el outbox deja el evento pendiente para reintentarlo (el
     * {@code eventId} es el mismo en cada reintento, y el consumidor descarta los duplicados).
     *
     * @param evento evento construido con {@link AuditoriaAccionEvent#builder()}
     * @throws AuditoriaNoPublicadaException si Kafka no lo confirmó
     */
    void publicarConfirmado(AuditoriaAccionEvent evento);

    /**
     * Como {@link #publicarConfirmado(AuditoriaAccionEvent)}, pero en el topic indicado y con una espera acotada. Lo
     * usa el relay del outbox de auditoría ({@code auditoria.outbox.*}), que guarda el topic con cada evento. La
     * implementación por defecto ignora el topic y la espera: existe para no romper a quien implemente esta interfaz
     * por su cuenta.
     *
     * @param evento evento construido con {@link AuditoriaAccionEvent#builder()}
     * @param topic  topic donde publicarlo
     * @param espera espera máxima de la confirmación del broker
     * @throws AuditoriaNoPublicadaException si Kafka no lo confirmó dentro de la espera
     */
    default void publicarConfirmado(AuditoriaAccionEvent evento, String topic, Duration espera) {
        publicarConfirmado(evento);
    }
}
