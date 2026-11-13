package com.factoring.commons.outbox;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

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

    /**
     * Cuánto conserva el productor un registro antes de descartarlo ({@code delivery.timeout.ms} efectivo), para la
     * comprobación al arrancar de {@link ProductorOutbox#comprobar}: tiene que ser menor que {@code timeout-ack} para
     * que un intento vencido no se entregue después. Desde 0.6.2.
     *
     * @return el plazo de entrega, o vacío si este publicador no lo informa (por defecto)
     */
    default Optional<Duration> plazoDeEntrega() {
        return Optional.empty();
    }
}
