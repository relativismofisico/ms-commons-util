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
 * <p>Por defecto el mensaje lleva el sobre completo ({@link EventoOutbox}). Con
 * {@link Builder#compatibleConJsonSerializer(String)} (desde 0.6.1) lleva solo el payload, como lo escribiría el
 * {@code JsonSerializer} de Spring Kafka, con la cabecera {@value #CABECERA_TIPO_SPRING}: así lo lee sin cambios un
 * consumidor existente con {@code JsonDeserializer}.</p>
 *
 * @param nombre           nombre lógico estable (viaja en la cabecera {@code tipo})
 * @param version          versión del esquema del payload (1 o más)
 * @param clase            clase del payload
 * @param topic            topic de destino; puede ser un placeholder {@code ${...}} que se resuelve al arrancar
 * @param clave            función que calcula la clave Kafka y de orden a partir del payload; nunca un valor aleatorio
 * @param idTipoCompatible valor de la cabecera {@value #CABECERA_TIPO_SPRING} si el tipo sale en formato compatible
 *                         con {@code JsonSerializer}; {@code null} si sale como sobre (lo normal)
 * @param <T>              tipo del payload
 */
public record TipoDeEvento<T>(String nombre, int version, Class<T> clase, String topic, Function<T, String> clave,
        String idTipoCompatible) {

    /** Cabecera de tipo de Spring Kafka, la que lee el {@code JsonDeserializer} del consumidor. */
    public static final String CABECERA_TIPO_SPRING = "__TypeId__";

    /**
     * Un tipo que sale como sobre (la forma de 0.6.0).
     *
     * @param nombre  nombre lógico estable
     * @param version versión del esquema del payload
     * @param clase   clase del payload
     * @param topic   topic de destino o placeholder
     * @param clave   función de la clave
     */
    public TipoDeEvento(final String nombre, final int version, final Class<T> clase, final String topic,
            final Function<T, String> clave) {
        this(nombre, version, clase, topic, clave, null);
    }

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
        return new TipoDeEvento<>(nombre, version, clase, topicResuelto, clave, idTipoCompatible);
    }

    /**
     * Si el mensaje sale en formato compatible con {@code JsonSerializer} (solo el payload y
     * {@value #CABECERA_TIPO_SPRING}) en lugar del sobre.
     *
     * @return si es compatible
     */
    public boolean compatibleConJsonSerializer() {
        return idTipoCompatible != null;
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
        private String idTipoCompatible;

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
         * El mensaje sale en formato compatible con el {@code JsonSerializer} de Spring Kafka, con la cabecera
         * {@value TipoDeEvento#CABECERA_TIPO_SPRING} igual al nombre completo de la clase del payload.
         *
         * @return el builder
         * @see #compatibleConJsonSerializer(String)
         */
        public Builder<T> compatibleConJsonSerializer() {
            return compatibleConJsonSerializer(clase.getName());
        }

        /**
         * El mensaje sale en formato compatible con el {@code JsonSerializer} de Spring Kafka, para un consumidor que
         * ya lee este topic con {@code JsonDeserializer}:
         * <ul>
         *   <li>valor: solo el payload, con los mismos bytes que el {@code JsonSerializer} por defecto;</li>
         *   <li>cabeceras: las del sobre ({@code eventId}, {@code tipo}, {@code version}, {@code servicioOrigen}) y
         *       {@value TipoDeEvento#CABECERA_TIPO_SPRING} con este valor;</li>
         *   <li>clave: la del tipo, como siempre.</li>
         * </ul>
         * El sobre se sigue guardando completo en el almacén; la conversión la hace el publicador al enviar.
         *
         * @param idTipo valor de {@value TipoDeEvento#CABECERA_TIPO_SPRING}: el nombre completo de una clase o un
         *               alias, según el {@code spring.json.type.mapping} del consumidor
         * @return el builder
         */
        public Builder<T> compatibleConJsonSerializer(final String idTipo) {
            this.idTipoCompatible = Objects.requireNonNullElse(idTipo, "");
            return this;
        }

        /**
         * La declaración. Se valida en {@link TiposDeEvento}.
         *
         * @return el tipo
         */
        public TipoDeEvento<T> build() {
            return new TipoDeEvento<>(nombre, version, Objects.requireNonNull(clase, "clase del payload"), topic,
                    clave, idTipoCompatible);
        }
    }
}
