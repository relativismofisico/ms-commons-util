package com.factoring.commons.outbox;

import java.util.Objects;
import java.util.function.Function;

/**
 * Declaración de un tipo de evento del outbox: nombre, versión del payload, clase del payload, topic y cómo se
 * calcula la clave a partir del payload. Se declara en código (no en configuración) para que el compilador vigile la
 * clase y la función de la clave:
 *
 * <pre>{@code
 * TipoDeEvento.de("pedido.creado", PedidoCreado.class)
 *         .version(1)
 *         .topic("${topics.pedido-creado}")
 *         .clave(PedidoCreado::pedidoId)
 *         .build();
 * }</pre>
 *
 * @param nombre  nombre lógico estable (viaja en la cabecera {@code tipo})
 * @param version versión del esquema del payload (1 o más)
 * @param clase   clase del payload
 * @param topic   topic de destino; puede ser un placeholder {@code ${...}} que se resuelve al arrancar
 * @param clave   función que calcula la clave Kafka y de orden a partir del payload; nunca un valor aleatorio
 * @param <T>     tipo del payload
 */
public record TipoDeEvento<T>(String nombre, int version, Class<T> clase, String topic, Function<T, String> clave) {

    /**
     * Empieza la declaración de un tipo.
     *
     * @param nombre nombre lógico del tipo
     * @param clase  clase del payload
     * @param <T>    tipo del payload
     * @return el builder
     */
    public static <T> Builder<T> de(final String nombre, final Class<T> clase) {
        return new Builder<>(nombre, clase);
    }

    /**
     * El mismo tipo con el topic ya resuelto.
     *
     * @param topicResuelto topic sin placeholders
     * @return copia con el topic
     */
    TipoDeEvento<T> conTopic(final String topicResuelto) {
        return new TipoDeEvento<>(nombre, version, clase, topicResuelto, clave);
    }

    /**
     * Calcula la clave del payload.
     *
     * @param payload payload del tipo
     * @return la clave
     * @throws IllegalArgumentException si la función devuelve una clave nula o vacía
     */
    String claveDe(final Object payload) {
        final String valor = clave.apply(clase.cast(payload));
        if (valor == null || valor.isBlank()) {
            throw new IllegalArgumentException("La clave del evento " + nombre + " salió vacía");
        }
        return valor;
    }

    /**
     * Builder de {@link TipoDeEvento}.
     *
     * @param <T> tipo del payload
     */
    public static final class Builder<T> {

        private final String nombre;
        private final Class<T> clase;
        private int version = 1;
        private String topic;
        private Function<T, String> clave;

        private Builder(final String nombre, final Class<T> clase) {
            this.nombre = nombre;
            this.clase = clase;
        }

        /**
         * Versión del esquema del payload (1 por defecto).
         *
         * @param valor versión
         * @return el builder
         */
        public Builder<T> version(final int valor) {
            this.version = valor;
            return this;
        }

        /**
         * Topic de destino, o un placeholder {@code ${...}} de una propiedad.
         *
         * @param valor topic o placeholder
         * @return el builder
         */
        public Builder<T> topic(final String valor) {
            this.topic = valor;
            return this;
        }

        /**
         * Función que calcula la clave a partir del payload.
         *
         * @param funcion función de la clave
         * @return el builder
         */
        public Builder<T> clave(final Function<T, String> funcion) {
            this.clave = funcion;
            return this;
        }

        /**
         * La declaración. Se valida en {@link TiposDeEvento}.
         *
         * @return el tipo
         */
        public TipoDeEvento<T> build() {
            return new TipoDeEvento<>(nombre, version, Objects.requireNonNull(clase, "clase del payload"), topic,
                    clave);
        }
    }
}
