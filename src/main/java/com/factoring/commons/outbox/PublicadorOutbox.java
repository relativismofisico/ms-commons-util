package com.factoring.commons.outbox;

import java.time.Duration;
import java.util.Map;

/**
 * Publica un evento genérico del outbox y espera la confirmación del broker. Lo usa el relay; la implementación por
 * defecto es {@link KafkaPublicadorOutbox}.
 */
public interface PublicadorOutbox {

    /**
     * Publica el valor con su clave y cabeceras, y espera el ack.
     *
     * @param topic      topic de destino
     * @param clave      clave del mensaje
     * @param valor      el sobre serializado
     * @param cabeceras  cabeceras del mensaje
     * @param espera     espera máxima de la confirmación
     * @throws OutboxNoPublicadoException si el broker no lo confirmó dentro de la espera
     */
    void publicarConfirmado(String topic, String clave, String valor, Map<String, String> cabeceras, Duration espera);
}
