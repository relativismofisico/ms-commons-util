package com.factoring.commons.outbox.idempotencia;

import java.time.Instant;

/**
 * Consumo idempotente: aplica el efecto de un evento a lo sumo una vez por consumidor, aunque el evento llegue
 * repetido (Kafka entrega al menos una vez). La marca {@code (consumidor, clave)} se inserta en la MISMA transacción
 * que el efecto: si el efecto falla, se revierten los dos y el reintento lo vuelve a aplicar; si la marca ya existe,
 * el efecto no corre.
 *
 * <p><strong>Solo sirve si el efecto está en el mismo almacén que la marca</strong> (la misma base y el mismo gestor
 * de transacciones). Para efectos externos (un correo, una llamada HTTP) no hay transacción común: ahí hay que
 * marcar después del efecto y aceptar un duplicado raro, o reclamar el efecto antes con estados propios.</p>
 *
 * <p>Esta marca es una segunda barrera: la primera son las guardas de estado del propio dominio (no repetir una
 * transición que ya ocurrió). Ver la retención en {@code consumo-idempotente.retencion}.</p>
 */
public interface ConsumoIdempotente {

    /** Prefijo de la clave cuando el evento no trae {@code eventId} y se usa una clave natural. */
    String PREFIJO_CLAVE_NATURAL = "nat:";

    /** Largo máximo del nombre del consumidor. */
    int LARGO_MAXIMO_CONSUMIDOR = 100;

    /** Largo máximo de la clave (con el prefijo). */
    int LARGO_MAXIMO_CLAVE = 191;

    /**
     * Aplica el efecto si el consumidor no procesó antes el evento.
     *
     * @param consumidor nombre lógico y estable del consumidor
     * @param eventId    {@code eventId} del evento
     * @param efecto     el efecto, que escribe en el mismo almacén dentro de la transacción en curso
     * @return {@code true} si aplicó el efecto; {@code false} si era un duplicado
     * @throws IllegalArgumentException si falta el consumidor o el {@code eventId}, o son demasiado largos
     */
    boolean procesar(String consumidor, String eventId, Runnable efecto);

    /**
     * Como {@link #procesar(String, String, Runnable)}; sin {@code eventId} (productores que aún no lo envían) usa la
     * clave natural con el prefijo {@value #PREFIJO_CLAVE_NATURAL}.
     *
     * @param consumidor   nombre lógico y estable del consumidor
     * @param eventId      {@code eventId} del evento, o {@code null}
     * @param claveNatural clave natural del evento (la que lo identifica en el dominio), usada si no hay
     *                     {@code eventId}
     * @param efecto       el efecto
     * @return {@code true} si aplicó el efecto; {@code false} si era un duplicado
     * @throws IllegalArgumentException si no hay ni {@code eventId} ni clave natural
     */
    default boolean procesar(final String consumidor, final String eventId, final String claveNatural,
            final Runnable efecto) {
        return procesar(consumidor, clave(eventId, claveNatural), efecto);
    }

    /**
     * Borra las marcas procesadas antes de {@code limite}.
     *
     * @param limite fecha límite
     * @param maximo máximo de marcas a borrar en esta llamada
     * @return marcas borradas
     */
    int limpiarAnteriores(Instant limite, int maximo);

    /**
     * La clave de la marca: el {@code eventId}, o la clave natural con el prefijo {@value #PREFIJO_CLAVE_NATURAL}.
     *
     * @param eventId      {@code eventId} del evento, o {@code null}
     * @param claveNatural clave natural, usada si no hay {@code eventId}
     * @return la clave
     * @throws IllegalArgumentException si no hay ninguna de las dos
     */
    static String clave(final String eventId, final String claveNatural) {
        if (eventId != null && !eventId.isBlank()) {
            return eventId;
        }
        if (claveNatural == null || claveNatural.isBlank()) {
            throw new IllegalArgumentException("Evento sin eventId ni clave natural");
        }
        return PREFIJO_CLAVE_NATURAL + claveNatural;
    }

    /**
     * Valida consumidor y clave.
     *
     * @param consumidor nombre del consumidor
     * @param clave      clave de la marca
     * @throws IllegalArgumentException si falta alguno o es demasiado largo
     */
    static void validar(final String consumidor, final String clave) {
        if (consumidor == null || consumidor.isBlank() || consumidor.length() > LARGO_MAXIMO_CONSUMIDOR) {
            throw new IllegalArgumentException("Consumidor vacío o de más de " + LARGO_MAXIMO_CONSUMIDOR
                    + " caracteres");
        }
        if (clave == null || clave.isBlank() || clave.length() > LARGO_MAXIMO_CLAVE) {
            throw new IllegalArgumentException("Clave del evento vacía o de más de " + LARGO_MAXIMO_CLAVE
                    + " caracteres");
        }
    }
}
