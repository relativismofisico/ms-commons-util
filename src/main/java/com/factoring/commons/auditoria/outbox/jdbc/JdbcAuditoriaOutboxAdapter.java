package com.factoring.commons.auditoria.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento;
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort;
import com.factoring.commons.auditoria.outbox.EstadoOutbox;
import com.factoring.commons.auditoria.outbox.OrdenPorClave;
import com.factoring.commons.auditoria.outbox.ResumenOutbox;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * {@link AuditoriaOutboxPort} sobre la tabla {@value #TABLA} del DataSource del servicio (MySQL 8; la migración la pone
 * cada servicio, el SQL de referencia está en el README).
 *
 * <p>Las fechas se guardan y comparan en UTC, sin depender de la zona de la JVM ni de la sesión de MySQL.</p>
 *
 * <p>{@link #guardar} usa el {@code JdbcTemplate} sobre el mismo DataSource, así participa en la transacción activa
 * (la de {@code JpaTransactionManager} o {@code DataSourceTransactionManager}). El reclamo del lote corre en una
 * transacción propia y corta, en READ COMMITTED (sin gap locks que frenen los INSERT de los casos de uso), con
 * {@code SELECT ... FOR UPDATE SKIP LOCKED}: dos relays nunca reclaman la misma fila. La publicación ocurre después,
 * fuera de esa transacción.</p>
 */
public class JdbcAuditoriaOutboxAdapter implements AuditoriaOutboxPort {

    /** Tabla del outbox. */
    public static final String TABLA = "auditoria_outbox";

    private static final String COLUMNAS = "id, event_id, clave_kafka, topic, payload, estado, intentos, "
            + "proximo_intento, lease_hasta, fecha_creacion, fecha_envio, ultimo_error";

    private static final String INSERTAR = "INSERT INTO " + TABLA + " (event_id, clave_kafka, topic, payload, estado, "
            + "intentos, proximo_intento, fecha_creacion) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    /**
     * Candidatos: pendientes listos sin un anterior de su clave que NO pueda ir en este lote (FALLIDO, EN_PROCESO o
     * PENDIENTE todavía en backoff). Así una clave bloqueada no ocupa el LIMIT: sus sucesores ni siquiera son
     * candidatos, y los de otras claves sí. Un anterior pendiente y listo no excluye: puede ir en el mismo lote (y si
     * no entra, {@link OrdenPorClave} lo resuelve después). El NOT EXISTS usa el índice (clave_kafka, estado, id) y es
     * una lectura sin bloqueo: FOR UPDATE solo bloquea las filas del lote.
     */
    private static final String CANDIDATOS = "SELECT " + COLUMNAS + " FROM " + TABLA + " o"
            + " WHERE o.estado = 'PENDIENTE' AND o.proximo_intento <= ?"
            + " AND NOT EXISTS (SELECT 1 FROM " + TABLA + " p WHERE p.clave_kafka = o.clave_kafka AND p.id < o.id"
            + " AND (p.estado IN ('FALLIDO', 'EN_PROCESO') OR (p.estado = 'PENDIENTE' AND p.proximo_intento > ?)))"
            + " ORDER BY o.id LIMIT ? FOR UPDATE SKIP LOCKED";

    private static final String NO_ENVIADOS_DE_CLAVES = "SELECT id, clave_kafka FROM " + TABLA
            + " WHERE clave_kafka IN (:claves) AND estado <> 'ENVIADO' AND id <= :hasta ORDER BY id";

    private static final String RECLAMAR = "UPDATE " + TABLA
            + " SET estado = 'EN_PROCESO', lease_hasta = :lease WHERE id IN (:ids)";

    private static final String VENCIDOS = "SELECT " + COLUMNAS + " FROM " + TABLA
            + " WHERE estado = 'EN_PROCESO' AND lease_hasta <= ? ORDER BY id LIMIT ?";

    private static final String ENVIADO = "UPDATE " + TABLA
            + " SET estado = 'ENVIADO', fecha_envio = ?, lease_hasta = NULL WHERE id = ? AND estado <> 'ENVIADO'";

    private static final String CON_LEASE = " WHERE id = ? AND estado = 'EN_PROCESO' AND lease_hasta = ?";

    private static final String REINTENTO = "UPDATE " + TABLA + " SET estado = 'PENDIENTE', intentos = ?, "
            + "proximo_intento = ?, lease_hasta = NULL, ultimo_error = ?" + CON_LEASE;

    private static final String FALLIDO = "UPDATE " + TABLA
            + " SET estado = 'FALLIDO', intentos = ?, lease_hasta = NULL, ultimo_error = ?" + CON_LEASE;

    private static final String LIBERAR = "UPDATE " + TABLA + " SET estado = 'PENDIENTE', lease_hasta = NULL"
            + CON_LEASE;

    private static final String RESUMEN = "SELECT"
            + " COALESCE(SUM(CASE WHEN estado IN ('PENDIENTE', 'EN_PROCESO') THEN 1 ELSE 0 END), 0) AS pendientes,"
            + " COALESCE(SUM(CASE WHEN estado = 'FALLIDO' THEN 1 ELSE 0 END), 0) AS fallidos,"
            + " MIN(CASE WHEN estado IN ('PENDIENTE', 'EN_PROCESO') THEN fecha_creacion END) AS mas_antiguo"
            + " FROM " + TABLA + " WHERE estado <> 'ENVIADO'";

    private static final String LIMPIAR = "DELETE FROM " + TABLA
            + " WHERE estado = 'ENVIADO' AND fecha_envio < ? ORDER BY id LIMIT ?";

    private static final RowMapper<AuditoriaOutboxEvento> MAPEO = (rs, fila) -> mapear(rs);

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate jdbcConNombres;
    private final TransactionTemplate transaccionDeReclamo;

    /**
     * @param dataSource el DataSource del servicio (el mismo de sus transacciones de negocio)
     */
    public JdbcAuditoriaOutboxAdapter(final DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.jdbcConNombres = new NamedParameterJdbcTemplate(jdbc);
        // Gestor propio y no expuesto como bean: solo para el reclamo, que corre en el hilo del relay sin otra
        // transacción en curso. Exponerlo competiría con el gestor de transacciones del servicio.
        final TransactionTemplate plantilla = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        plantilla.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        plantilla.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transaccionDeReclamo = plantilla;
    }

    @Override
    public void guardar(final AuditoriaOutboxEvento evento) {
        jdbc.update(INSERTAR, evento.eventId(), evento.claveKafka(), evento.topic(), evento.payload(),
                evento.estado().name(), evento.intentos(), ts(evento.proximoIntento()), ts(evento.fechaCreacion()));
    }

    @Override
    public List<AuditoriaOutboxEvento> tomarLote(final int tamano, final Instant ahora, final Instant leaseHasta) {
        final List<AuditoriaOutboxEvento> lote = transaccionDeReclamo.execute(estado -> reclamar(tamano, ahora,
                leaseHasta));
        if (lote == null) {
            return List.of();
        }
        return lote;
    }

    private List<AuditoriaOutboxEvento> reclamar(final int tamano, final Instant ahora, final Instant leaseHasta) {
        final List<AuditoriaOutboxEvento> candidatos = jdbc.query(CANDIDATOS, MAPEO, ts(ahora), ts(ahora), tamano);
        if (candidatos.isEmpty()) {
            return List.of();
        }
        final Set<String> idsCandidatos = new LinkedHashSet<>();
        final Set<String> claves = new LinkedHashSet<>();
        for (final AuditoriaOutboxEvento candidato : candidatos) {
            idsCandidatos.add(candidato.id());
            claves.add(candidato.claveKafka());
        }
        final String ultimo = candidatos.get(candidatos.size() - 1).id();
        final List<OrdenPorClave.Fila> filas = jdbcConNombres.query(NO_ENVIADOS_DE_CLAVES,
                Map.of("claves", claves, "hasta", Long.parseLong(ultimo)),
                (rs, n) -> new OrdenPorClave.Fila(rs.getString("id"), rs.getString("clave_kafka"),
                        idsCandidatos.contains(rs.getString("id"))));
        final Set<String> elegibles = OrdenPorClave.elegibles(filas);
        if (elegibles.isEmpty()) {
            return List.of();
        }
        final Instant lease = truncar(leaseHasta);
        final List<Long> ids = new ArrayList<>();
        final List<AuditoriaOutboxEvento> reclamados = new ArrayList<>();
        for (final AuditoriaOutboxEvento candidato : candidatos) {
            if (elegibles.contains(candidato.id())) {
                ids.add(Long.valueOf(candidato.id()));
                reclamados.add(candidato.enProceso(lease));
            }
        }
        jdbcConNombres.update(RECLAMAR, Map.of("lease", ts(lease), "ids", ids));
        return reclamados;
    }

    @Override
    public List<AuditoriaOutboxEvento> buscarLeasesVencidos(final Instant ahora, final int limite) {
        return jdbc.query(VENCIDOS, MAPEO, ts(ahora), limite);
    }

    @Override
    public boolean marcarEnviado(final String id, final Instant fechaEnvio) {
        return jdbc.update(ENVIADO, ts(fechaEnvio), Long.valueOf(id)) > 0;
    }

    @Override
    public boolean marcarReintento(final AuditoriaOutboxEvento evento, final int intentos,
            final Instant proximoIntento, final String error) {
        return jdbc.update(REINTENTO, intentos, ts(proximoIntento), error, Long.valueOf(evento.id()),
                ts(evento.leaseHasta())) > 0;
    }

    @Override
    public boolean marcarFallido(final AuditoriaOutboxEvento evento, final int intentos, final String error) {
        return jdbc.update(FALLIDO, intentos, error, Long.valueOf(evento.id()), ts(evento.leaseHasta())) > 0;
    }

    @Override
    public boolean liberar(final AuditoriaOutboxEvento evento) {
        return jdbc.update(LIBERAR, Long.valueOf(evento.id()), ts(evento.leaseHasta())) > 0;
    }

    @Override
    public ResumenOutbox resumen() {
        final ResumenOutbox resumen = jdbc.queryForObject(RESUMEN, (rs, fila) -> new ResumenOutbox(
                rs.getLong("pendientes"), rs.getLong("fallidos"), instante(rs, "mas_antiguo")));
        if (resumen == null) {
            return ResumenOutbox.VACIO;
        }
        return resumen;
    }

    @Override
    public int limpiarEnviadosAnteriores(final Instant limite, final int maximo) {
        return jdbc.update(LIMPIAR, ts(limite), maximo);
    }

    /** DATETIME(3): todas las fechas se guardan y comparan en milisegundos (el lease se compara por igualdad). */
    private static Instant truncar(final Instant instante) {
        return instante.truncatedTo(ChronoUnit.MILLIS);
    }

    /**
     * Las fechas viajan como {@link LocalDateTime} en UTC: Connector/J no convierte LocalDateTime (ni con la zona de la
     * JVM ni con la de la sesión), y DATETIME no tiene zona. {@code Timestamp} sí se convertiría con la zona de la JVM.
     */
    private static LocalDateTime ts(final Instant instante) {
        if (instante == null) {
            return null;
        }
        return LocalDateTime.ofInstant(truncar(instante), ZoneOffset.UTC);
    }

    private static Instant instante(final ResultSet rs, final String columna) throws SQLException {
        final LocalDateTime valor = rs.getObject(columna, LocalDateTime.class);
        if (valor == null) {
            return null;
        }
        return valor.toInstant(ZoneOffset.UTC);
    }

    private static AuditoriaOutboxEvento mapear(final ResultSet rs) throws SQLException {
        return new AuditoriaOutboxEvento(rs.getString("id"), rs.getString("event_id"), rs.getString("clave_kafka"),
                rs.getString("topic"), rs.getString("payload"), EstadoOutbox.valueOf(rs.getString("estado")),
                rs.getInt("intentos"), instante(rs, "proximo_intento"), instante(rs, "lease_hasta"),
                instante(rs, "fecha_creacion"), instante(rs, "fecha_envio"), rs.getString("ultimo_error"));
    }
}
