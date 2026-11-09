package com.factoring.commons.outbox;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Los tipos de evento que un servicio puede registrar en el outbox. El servicio expone un único bean de esta clase;
 * la autoconfiguración lo valida al arrancar (fail-fast) y {@link OutboxPort#registrar} rechaza en ejecución cualquier
 * tipo que no esté aquí.
 *
 * <pre>{@code
 * @Bean
 * TiposDeEvento tiposDeEvento() {
 *     return TiposDeEvento.de(
 *             TipoDeEvento.de("pedido.creado", PedidoCreado.class).topic("${topics.pedido}")
 *                     .clave(PedidoCreado::pedidoId).build());
 * }
 * }</pre>
 */
public final class TiposDeEvento {

    private static final String PLACEHOLDER = "${";

    private final Map<String, TipoDeEvento<?>> porNombre;

    private TiposDeEvento(final Map<String, TipoDeEvento<?>> porNombre) {
        this.porNombre = porNombre;
    }

    /**
     * Declara los tipos. Valida lo que no depende del entorno: nombres únicos y no vacíos, versión 1 o más, topic y
     * función de clave presentes, y un {@code __TypeId__} no vacío en los tipos compatibles con {@code JsonSerializer}.
     *
     * @param tipos tipos del servicio
     * @return el registro
     * @throws IllegalStateException con el problema de la declaración
     */
    public static TiposDeEvento de(final TipoDeEvento<?>... tipos) {
        return de(List.of(tipos));
    }

    /**
     * Como {@link #de(TipoDeEvento[])}, con una colección.
     *
     * @param tipos tipos del servicio
     * @return el registro
     * @throws IllegalStateException con el problema de la declaración
     */
    public static TiposDeEvento de(final Collection<TipoDeEvento<?>> tipos) {
        final Map<String, TipoDeEvento<?>> porNombre = new LinkedHashMap<>();
        for (final TipoDeEvento<?> tipo : tipos) {
            exigir(tipo.nombre() != null && !tipo.nombre().isBlank(), "un tipo de evento sin nombre");
            exigir(tipo.version() > 0, "el tipo " + tipo.nombre() + " tiene que tener versión 1 o más");
            exigir(tipo.topic() != null && !tipo.topic().isBlank(), "el tipo " + tipo.nombre() + " no tiene topic");
            exigir(tipo.clave() != null, "el tipo " + tipo.nombre() + " no tiene función de clave");
            exigir(tipo.idTipoCompatible() == null || !tipo.idTipoCompatible().isBlank(),
                    "el tipo " + tipo.nombre() + " es compatible con JsonSerializer pero su "
                            + TipoDeEvento.CABECERA_TIPO_SPRING + " está vacío");
            exigir(porNombre.put(tipo.nombre(), tipo) == null, "el tipo " + tipo.nombre() + " está declarado dos veces");
        }
        exigir(!porNombre.isEmpty(), "no hay tipos de evento declarados");
        return new TiposDeEvento(Map.copyOf(porNombre));
    }

    /**
     * Los mismos tipos con los topics resueltos (placeholders {@code ${...}} de propiedades).
     *
     * @param resolver resuelve los placeholders del topic (p. ej. {@code Environment::resolveRequiredPlaceholders})
     * @return el registro resuelto
     * @throws IllegalStateException si un topic no se puede resolver o queda vacío
     */
    public TiposDeEvento resolver(final UnaryOperator<String> resolver) {
        final Map<String, TipoDeEvento<?>> resueltos = new LinkedHashMap<>();
        for (final TipoDeEvento<?> tipo : porNombre.values()) {
            final String topic;
            try {
                topic = resolver.apply(tipo.topic());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("El topic del tipo " + tipo.nombre() + " no se pudo resolver", e);
            }
            exigir(topic != null && !topic.isBlank() && !topic.contains(PLACEHOLDER),
                    "el topic del tipo " + tipo.nombre() + " quedó vacío o sin resolver");
            resueltos.put(tipo.nombre(), tipo.conTopic(topic));
        }
        return new TiposDeEvento(Map.copyOf(resueltos));
    }

    /**
     * El tipo declarado con ese nombre.
     *
     * @param nombre nombre del tipo
     * @return la declaración
     * @throws TipoDeEventoNoDeclaradoException si no está declarado
     */
    public TipoDeEvento<?> requerir(final String nombre) {
        final TipoDeEvento<?> tipo = porNombre.get(nombre);
        if (tipo == null) {
            throw new TipoDeEventoNoDeclaradoException(nombre);
        }
        return tipo;
    }

    /**
     * Nombres de los tipos declarados.
     *
     * @return los nombres
     */
    public Set<String> nombres() {
        return porNombre.keySet();
    }

    private static void exigir(final boolean condicion, final String mensaje) {
        if (!condicion) {
            throw new IllegalStateException("TiposDeEvento: " + mensaje);
        }
    }
}
