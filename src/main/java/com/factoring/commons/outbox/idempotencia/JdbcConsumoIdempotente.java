package com.factoring.commons.outbox.idempotencia;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Pattern;
import javax.sql.DataSource;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * {@link ConsumoIdempotente} sobre una tabla del DataSource del servicio (MySQL 8; SQL de referencia en el README).
 * Usa el gestor de transacciones del servicio con propagación REQUIRED: la marca y el efecto se confirman juntos, y si
 * ya hay una transacción en curso se une a ella.
 *
 * <p>Dos entregas concurrentes del mismo evento: el segundo INSERT espera el bloqueo de la clave primaria; si la
 * primera confirma, recibe clave duplicada y no aplica el efecto; si la primera se revierte, la segunda lo aplica.
 * En MySQL un INSERT duplicado solo revierte esa sentencia, así que la transacción sigue sana y se confirma vacía.</p>
 */
public final class JdbcConsumoIdempotente implements ConsumoIdempotente {

    /** Tabla por defecto. */
    public static final String TABLA = "evento_procesado";

    private static final Pattern NOMBRE_VALIDO = Pattern.compile("[A-Za-z][A-Za-z0-9_]{0,63}");

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaccion;
    private final String tabla;
    private final Clock reloj;

    /**
     * @param dataSource  el DataSource del servicio
     * @param transacciones el gestor de transacciones del servicio sobre ese DataSource (JPA o JDBC)
     * @param tabla       tabla de las marcas
     * @param reloj       reloj de {@code fecha_procesado}
     * @throws IllegalArgumentException si el nombre de la tabla no es válido
     */
    public JdbcConsumoIdempotente(final DataSource dataSource, final PlatformTransactionManager transacciones,
            final String tabla, final Clock reloj) {
        this.tabla = validarTabla(tabla);
        this.jdbc = new JdbcTemplate(dataSource);
        this.transaccion = new TransactionTemplate(transacciones);
        this.reloj = reloj;
    }

    /** El nombre se concatena en el SQL: se valida antes de usarlo. */
    private static String validarTabla(final String tabla) {
        if (tabla == null || !NOMBRE_VALIDO.matcher(tabla).matches()) {
            throw new IllegalArgumentException("Nombre de tabla inválido: " + tabla);
        }
        return tabla;
    }

    @Override
    public boolean procesar(final String consumidor, final String eventId, final Runnable efecto) {
        ConsumoIdempotente.validar(consumidor, eventId);
        final Boolean aplicado = transaccion.execute(estado -> {
            try {
                jdbc.update("INSERT INTO " + tabla + " (consumidor, clave, fecha_procesado) VALUES (?, ?, ?)",
                        consumidor, eventId, LocalDateTime.ofInstant(reloj.instant(), ZoneOffset.UTC));
            } catch (DuplicateKeyException duplicado) {
                return Boolean.FALSE;
            }
            efecto.run();
            return Boolean.TRUE;
        });
        return Boolean.TRUE.equals(aplicado);
    }

    @Override
    public int limpiarAnteriores(final Instant limite, final int maximo) {
        return jdbc.update("DELETE FROM " + tabla + " WHERE fecha_procesado < ? LIMIT ?",
                LocalDateTime.ofInstant(limite, ZoneOffset.UTC), maximo);
    }
}
