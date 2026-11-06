package com.factoring.commons.auditoria.outbox;

import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades {@code auditoria.outbox.*}. Con {@code enabled=false} (por defecto) no se registra nada y el servicio
 * sigue publicando la auditoría como en 0.4.x.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = AuditoriaOutboxProperties.PREFIJO)
public class AuditoriaOutboxProperties {

    /** Prefijo de las propiedades. */
    public static final String PREFIJO = "auditoria.outbox";

    private static final int LOTE_POR_DEFECTO = 50;
    private static final int INTENTOS_POR_DEFECTO = 10;
    private static final int RETENCION_DIAS_POR_DEFECTO = 30;
    private static final int ESPERA_ACK_S_POR_DEFECTO = 30;
    private static final int BACKOFF_MAX_MIN_POR_DEFECTO = 10;
    private static final int CINCO = 5;
    private static final int METRICAS_S_POR_DEFECTO = 30;

    /** Almacenes soportados. */
    public enum Almacen {
        /** Tabla {@code auditoria_outbox} en el DataSource del servicio. */
        JDBC,
        /** Colección propia del servicio en su MongoDB. */
        MONGO
    }

    /** Activa el outbox. */
    private boolean enabled;

    /** Dónde se guarda el outbox: {@code jdbc} o {@code mongo}. Obligatorio con {@code enabled=true}. */
    private Almacen almacen;

    /** Eventos que el relay reclama por ciclo. */
    private int tamanoLote = LOTE_POR_DEFECTO;

    /** Pausa entre ciclos del relay. */
    private Duration intervalo = Duration.ofSeconds(CINCO);

    /** Publicaciones fallidas tras las que el evento pasa a FALLIDO. */
    private int maxIntentos = INTENTOS_POR_DEFECTO;

    /** Espera tras el primer fallo; se duplica en cada intento. */
    private Duration backoffBase = Duration.ofSeconds(CINCO);

    /** Tope de la espera entre intentos. */
    private Duration backoffMax = Duration.ofMinutes(BACKOFF_MAX_MIN_POR_DEFECTO);

    /** Cuánto se conservan los eventos ENVIADO antes de borrarlos. */
    private Duration retencion = Duration.ofDays(RETENCION_DIAS_POR_DEFECTO);

    /** Pausa entre limpiezas. */
    private Duration intervaloLimpieza = Duration.ofHours(1);

    /** Pausa entre lecturas del almacén para las métricas (solo con Micrometer). */
    private Duration intervaloMetricas = Duration.ofSeconds(METRICAS_S_POR_DEFECTO);

    /** Cuánto tiempo un relay retiene un lote reclamado; tiene que superar a {@code timeoutAck}. */
    private Duration lease = Duration.ofMinutes(CINCO);

    /** Espera máxima de la confirmación de Kafka por evento. */
    private Duration timeoutAck = Duration.ofSeconds(ESPERA_ACK_S_POR_DEFECTO);

    /** Opciones del almacén Mongo. */
    private Mongo mongo = new Mongo();

    /**
     * Comprueba que la configuración sea coherente.
     *
     * @throws IllegalStateException con la propiedad que falta o sobra
     */
    public void validar() {
        if (almacen == null) {
            throw new IllegalStateException(PREFIJO + ".almacen es obligatorio con " + PREFIJO
                    + ".enabled=true (jdbc o mongo)");
        }
        exigir(tamanoLote > 0, "tamano-lote tiene que ser mayor que 0");
        exigir(maxIntentos > 0, "max-intentos tiene que ser mayor que 0");
        exigirPositiva(intervalo, "intervalo");
        exigirPositiva(intervaloLimpieza, "intervalo-limpieza");
        exigirPositiva(intervaloMetricas, "intervalo-metricas");
        exigirPositiva(backoffBase, "backoff-base");
        exigirPositiva(retencion, "retencion");
        exigirPositiva(timeoutAck, "timeout-ack");
        exigir(backoffMax.compareTo(backoffBase) >= 0, "backoff-max no puede ser menor que backoff-base");
        exigir(lease.compareTo(timeoutAck) > 0, "lease tiene que ser mayor que timeout-ack");
    }

    private static void exigirPositiva(final Duration duracion, final String nombre) {
        exigir(duracion != null && !duracion.isNegative() && !duracion.isZero(), nombre + " tiene que ser mayor que 0");
    }

    private static void exigir(final boolean condicion, final String mensaje) {
        if (!condicion) {
            throw new IllegalStateException(PREFIJO + "." + mensaje);
        }
    }

    /** Opciones del almacén Mongo. */
    @Getter
    @Setter
    public static class Mongo {

        /**
         * Colección del outbox. Por defecto {@code auditoria_outbox_<spring.application.name>}: nunca la comparten dos
         * servicios, aunque usen la misma base.
         */
        private String coleccion;
    }
}
