package com.factoring.commons.outbox.idempotencia;

import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades {@code consumo-idempotente.*}. Con {@code enabled=false} (por defecto) no se registra nada.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = ConsumoIdempotenteProperties.PREFIJO)
public class ConsumoIdempotenteProperties {

    /** Prefijo de las propiedades. */
    public static final String PREFIJO = "consumo-idempotente";

    private static final int RETENCION_DIAS_POR_DEFECTO = 30;

    /** Almacenes soportados. */
    public enum Almacen {
        /** Tabla en el DataSource del servicio. */
        JDBC,
        /** Colección en el MongoDB del servicio. */
        MONGO
    }

    /** Activa el consumo idempotente. */
    private boolean enabled;

    /** Dónde se guardan las marcas: {@code jdbc} o {@code mongo}. Obligatorio con {@code enabled=true}. */
    private Almacen almacen;

    /**
     * Cuánto se conserva cada marca. Tiene que ser mayor que la retención de los topics que consume (un consumidor
     * que rebobina reentrega todo lo que el topic guarda) y que lo que tarde en resolverse a mano un evento fallido
     * que se reintenta con el mismo {@code eventId}. 30 días por defecto.
     */
    private Duration retencion = Duration.ofDays(RETENCION_DIAS_POR_DEFECTO);

    /** Pausa entre limpiezas de las marcas vencidas (almacén JDBC; en Mongo las borra el índice TTL). */
    private Duration intervaloLimpieza = Duration.ofHours(1);

    /** Tabla de las marcas (almacén JDBC). */
    private String tabla = JdbcConsumoIdempotente.TABLA;

    /** Colección de las marcas (almacén Mongo). */
    private String coleccion = MongoConsumoIdempotente.COLECCION;

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
        if (retencion == null || retencion.isNegative() || retencion.isZero()) {
            throw new IllegalStateException(PREFIJO + ".retencion tiene que ser mayor que 0");
        }
        if (intervaloLimpieza == null || intervaloLimpieza.isNegative() || intervaloLimpieza.isZero()) {
            throw new IllegalStateException(PREFIJO + ".intervalo-limpieza tiene que ser mayor que 0");
        }
    }
}
