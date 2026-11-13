package com.factoring.commons.outbox;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeouts del productor Kafka de un publicador del outbox, derivados de {@code timeout-ack} (desde 0.6.2).
 *
 * <p>El relay espera la confirmación de cada envío como mucho {@code timeout-ack}. Si el productor conserva el registro
 * más tiempo que eso ({@code delivery.timeout.ms} vale 120 s por defecto), un intento que el relay dio por fallido
 * puede entregarse igual cuando el broker vuelve, y el reintento siguiente sale como una copia más. Con
 * {@link #acotarTimeouts} el productor descarta el registro antes de que el relay se rinda: un intento vencido queda
 * definitivamente fallido y no se entrega después.</p>
 *
 * <p>Lo que NO cubre: un lote que ya salió hacia el broker y cuya respuesta no llegó (por ejemplo, un broker que se
 * congela con la conexión abierta) tiene resultado desconocido; si el broker lo escribió, el reintento del relay es
 * un envío nuevo y el consumidor verá dos mensajes con el mismo {@code eventId}. Ese caso lo resuelve la
 * deduplicación del consumidor por {@code eventId}.</p>
 */
public final class ProductorOutbox {

    /** Margen mínimo entre {@code delivery.timeout.ms} y {@code timeout-ack} (nunca más de la mitad de este). */
    static final long MARGEN_MINIMO_MS = 100;

    /** El margen es la décima parte de {@code timeout-ack} (con el mínimo de {@link #MARGEN_MINIMO_MS}). */
    static final int DIVISOR_MARGEN = 10;

    private static final Logger LOG = LoggerFactory.getLogger(ProductorOutbox.class);

    private ProductorOutbox() {
    }

    /**
     * Acota, en el mapa de propiedades de un productor, los timeouts que deciden cuánto vive un registro:
     * <ul>
     *   <li>{@code delivery.timeout.ms}: el menor entre el configurado y {@code timeout-ack} menos un margen (la
     *       décima parte, al menos {@value #MARGEN_MINIMO_MS} ms y nunca más de la mitad), de modo que el envío se resuelve, con éxito o con
     *       error, antes de que el relay deje de esperarlo;</li>
     *   <li>{@code request.timeout.ms}: el menor entre el configurado y {@code delivery.timeout.ms - linger.ms}, porque
     *       Kafka exige {@code delivery.timeout.ms >= linger.ms + request.timeout.ms};</li>
     *   <li>{@code max.block.ms}: el menor entre el configurado y {@code delivery.timeout.ms}, para que un broker sin
     *       metadata no retenga al relay más que un intento.</li>
     * </ul>
     * Un valor configurado más bajo se respeta. Los valores ausentes se toman de los de Kafka por defecto.
     *
     * @param propiedades propiedades del productor (se modifican en el sitio)
     * @param timeoutAck  espera máxima de la confirmación en el relay ({@code auditoria.outbox.timeout-ack})
     * @return el mismo mapa, para encadenar
     * @throws IllegalArgumentException si {@code timeout-ack} no deja tiempo para {@code linger.ms}
     */
    public static Map<String, Object> acotarTimeouts(final Map<String, Object> propiedades, final Duration timeoutAck) {
        Objects.requireNonNull(propiedades, "propiedades");
        Objects.requireNonNull(timeoutAck, "timeoutAck");
        final long ack = timeoutAck.toMillis();
        final long limite = ack - Math.min(Math.max(ack / DIVISOR_MARGEN, MARGEN_MINIMO_MS), ack / 2);
        final long linger = valor(propiedades, ProducerConfig.LINGER_MS_CONFIG);
        final long entrega = Math.min(valor(propiedades, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), limite);
        final long peticion = Math.min(valor(propiedades, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), entrega - linger);
        if (peticion <= 0) {
            throw new IllegalArgumentException("timeout-ack=" + timeoutAck + " no deja tiempo de entrega con linger.ms="
                    + linger + ": suba timeout-ack o baje linger.ms");
        }
        final long bloqueo = Math.min(valor(propiedades, ProducerConfig.MAX_BLOCK_MS_CONFIG), entrega);
        propiedades.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Math.toIntExact(entrega));
        propiedades.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(peticion));
        propiedades.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, bloqueo);
        return propiedades;
    }

    /**
     * {@code delivery.timeout.ms} efectivo de unas propiedades de productor (el de Kafka por defecto si falta).
     *
     * @param propiedades propiedades del productor
     * @return el plazo de entrega
     */
    public static Duration plazoDeEntrega(final Map<String, Object> propiedades) {
        return Duration.ofMillis(valor(propiedades, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
    }

    /**
     * Comprobación al arrancar: avisa si el publicador del relay puede entregar un intento vencido. No impide el
     * arranque. Con un plazo de entrega que no es menor que {@code timeout-ack} registra un ERROR; si el publicador no
     * informa su plazo ({@link PublicadorOutbox#plazoDeEntrega()} vacío), un WARN.
     *
     * @param publicador el publicador del relay ({@code null}: nada que comprobar)
     * @param timeoutAck espera máxima de la confirmación en el relay
     * @return {@code true} si el plazo de entrega es menor que {@code timeout-ack}
     */
    public static boolean comprobar(final PublicadorOutbox publicador, final Duration timeoutAck) {
        if (publicador == null) {
            return true;
        }
        final String clase = publicador.getClass().getName();
        final Optional<Duration> plazo = publicador.plazoDeEntrega();
        if (plazo.isEmpty()) {
            LOG.warn("[OUTBOX][PRODUCTOR] {} no informa su delivery.timeout.ms: si supera timeout-ack={}, un intento "
                    + "vencido puede entregarse después (ver ProductorOutbox.acotarTimeouts)", clase, timeoutAck);
            return false;
        }
        if (plazo.get().compareTo(timeoutAck) >= 0) {
            LOG.error("[OUTBOX][PRODUCTOR] {}: delivery.timeout.ms={} ms no es menor que timeout-ack={}: un intento "
                    + "vencido puede entregarse después; aplique ProductorOutbox.acotarTimeouts a su productor",
                    clase, plazo.get().toMillis(), timeoutAck);
            return false;
        }
        LOG.info("[OUTBOX][PRODUCTOR] {}: delivery.timeout.ms={} ms, menor que timeout-ack={}", clase,
                plazo.get().toMillis(), timeoutAck);
        return true;
    }

    private static long valor(final Map<String, Object> propiedades, final String nombre) {
        Object valor = propiedades.get(nombre);
        if (valor == null) {
            valor = ProducerConfig.configDef().defaultValues().get(nombre);
        }
        if (valor instanceof Number numero) {
            return numero.longValue();
        }
        return Long.parseLong(String.valueOf(valor).trim());
    }
}
