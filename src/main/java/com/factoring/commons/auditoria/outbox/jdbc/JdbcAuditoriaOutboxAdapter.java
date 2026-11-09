package com.factoring.commons.auditoria.outbox.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.sql.DataSource;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento;
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort;
import com.factoring.commons.auditoria.outbox.EstadoOutbox;
import com.factoring.commons.auditoria.outbox.OrdenPorClave;
import com.factoring.commons.auditoria.outbox.ResumenOutbox;
import com.factoring.commons.auditoria.outbox.ResumenTipo;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * {@link AuditoriaOutboxPort} sobre una tabla del DataSource del servicio (MySQL 8; la migración la pone cada
 * servicio, el SQL de referencia está en el README). La tabla es configurable ({@value #TABLA} por defecto, el nombre
 * de 0.5.x).
 *
 * <p>Las fechas se guardan y comparan en UTC, sin depender de la zona de la JVM ni de la sesión de MySQL.</p>
 *
 * <p>{@link #guardar} usa el {@code JdbcTemplate} sobre el mismo DataSource, así participa en la transacción activa
 * (la de {@code JpaTransactionManager} o {@code DataSourceTransactionManager}). El reclamo del lote corre en una
 * transacción propia y corta, en READ COMMITTED (sin gap locks que frenen los INSERT de los casos de uso), con
 * {@code SELECT ... FOR UPDATE SKIP LOCKED}: dos relays nunca reclaman la misma fila. La publicación ocurre después,
 * fuera de esa transacción.</p>
 *
 * <p>Esquema: el de 0.5.x sirve para los eventos de auditoría. Los eventos genéricos necesitan además la columna
 * {@code tipo} (0.6.0); el adaptador detecta si existe la primera vez que la necesita.</p>
 */
public final class JdbcAuditoriaOutboxAdapter implements AuditoriaOutboxPort {

    /** Tabla del outbox por defecto (el nombre fijo de 0.5.x). */
    public static final String TABLA = "auditoria_outbox";

    private static final Pattern NOMBRE_VALIDO = Pattern.compile("[A-Za-z][A-Za-z0-9_]{0,63}");
    private static final String COLUMNAS_05 = "id, event_id, clave_kafka, topic, payload, estado, intentos, "
            + "proximo_intento, lease_hasta, fecha_creacion, fecha_envio, ultimo_error";
    private static final String EXISTE_COLUMNA_TIPO = "SELECT COUNT(*) FROM information_schema.columns"
            + " WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'tipo'";
    private static final String RESUMEN_COLUMNAS = " COALESCE(SUM(CASE WHEN estado IN ('PENDIENTE', 'EN_PROCESO')"
            + " THEN 1 ELSE 0 END), 0) AS pendientes,"
            + " COALESCE(SUM(CASE WHEN estado = 'FALLIDO' THEN 1 ELSE 0 END), 0) AS fallidos,"
            + " COALESCE(SUM(CASE WHEN estado = 'EN_PROCESO' THEN 1 ELSE 0 END), 0) AS en_proceso,"
            + " MIN(CASE WHEN estado IN ('PENDIENTE', 'EN_PROCESO') THEN fecha_creacion END) AS mas_antiguo";

    private final String tabla;
    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate jdbcConNombres;
    private final TransactionTemplate transaccionDeReclamo;
    private final AtomicReference<Boolean> conTipo = new AtomicReference<>();

    /**
     * Adaptador sobre la tabla {@value #TABLA}.
     *
     * @param dataSource el DataSource del servicio (el mismo de sus transacciones de negocio)
     */
    public JdbcAuditoriaOutboxAdapter(final DataSource dataSource) {
        this(dataSource, TABLA);
    }

    /**
     * @param dataSource el DataSource del servicio (el mismo de sus transacciones de negocio)
     * @param tabla      tabla del outbox (letras, dígitos y {@code _}; p. ej. {@code outbox_<servicio>})
     * @throws IllegalArgumentException si el nombre de la tabla no es válido
     */
    public JdbcAuditoriaOutboxAdapter(final DataSource dataSource, final String tabla) {
        this(validarTabla(tabla), dataSource);
    }

    private JdbcAuditoriaOutboxAdapter(final String tabla, final DataSource dataSource) {
        this.tabla = tabla;
        this.jdbc = new JdbcTemplate(dataSource);
        this.jdbcConNombres = new NamedParameterJdbcTemplate(jdbc);
        // Gestor propio y no expuesto como bean: solo para el reclamo, que corre en el hilo del relay sin otra
        // transacción en curso. Exponerlo competiría con el gestor de transacciones del servicio.
        final TransactionTemplate plantilla = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        plantilla.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        plantilla.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transaccionDeReclamo = plantilla;
    }

    /** El nombre se concatena en el SQL: se valida antes de construir nada. */
    private static String validarTabla(final String tabla) {
        if (tabla == null || !NOMBRE_VALIDO.matcher(tabla).matches()) {
            throw new IllegalArgumentException("Nombre de tabla del outbox inválido: " + tabla);
        }
        return tabla;
    }

    /**
     * Tabla del outbox.
     *
     * @return el nombre
     */
    public String getTabla() {
        return tabla;
    }

    /** Si la tabla tiene la columna {@code tipo} (esquema 0.6.0); se consulta una vez y se recuerda. */
    private boolean conTipo() {
        Boolean valor = conTipo.get();
        if (valor == null) {
            final Integer columnas = jdbc.queryForObject(EXISTE_COLUMNA_TIPO, Integer.class, tabla);
            valor = columnas != null && columnas > 0;
            conTipo.set(valor);
        }
        return valor;
    }

    private String columnas() {
        if (conTipo()) {
            return COLUMNAS_05 + ", tipo";
        }
        return COLUMNAS_05;
    }

    @Override
    public void verificarEventosGenericos() {
        if (!conTipo()) {
            throw new IllegalStateException("La tabla " + tabla + " del outbox no tiene la columna tipo: los eventos "
                    + "genéricos necesitan el esquema 0.6.0 (ver el README)");
        }
    }

    @Override
    public void guardar(final AuditoriaOutboxEvento evento) {
        if (conTipo()) {
            jdbc.update("INSERT INTO " + tabla + " (event_id, clave_kafka, topic, payload, estado, intentos, "
                            + "proximo_intento, fecha_creacion, tipo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    evento.eventId(), evento.claveKafka(), evento.topic(), evento.payload(), evento.estado().name(),
                    evento.intentos(), ts(evento.proximoIntento()), ts(evento.fechaCreacion()), evento.tipo());
            return;
        }
        if (evento.tipo() != null) {
            verificarEventosGenericos();
        }
        jdbc.update("INSERT INTO " + tabla + " (event_id, clave_kafka, topic, payload, estado, intentos, "
                        + "proximo_intento, fecha_creacion) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                evento.eventId(), evento.claveKafka(), evento.topic(), evento.payload(), evento.estado().name(),
                evento.intentos(), ts(evento.proximoIntento()), ts(evento.fechaCreacion()));
    }

    /**
     * {@inheritDoc}
     *
     * <p>En MySQL un error de clave duplicada revierte solo la sentencia, no la transacción: se intenta el INSERT y,
     * si choca con {@code ux_..._event_id}, se informa el duplicado.</p>
     */
    @Override
    public boolean guardarSiNoExiste(final AuditoriaOutboxEvento evento) {
        try {
            guardar(evento);
            return true;
        } catch (DuplicateKeyException duplicado) {
            return false;
        }
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

    /**
     * Candidatos: pendientes listos sin un anterior de su clave que NO pueda ir en este lote (FALLIDO, EN_PROCESO o
     * PENDIENTE todavía en backoff). Así una clave bloqueada no ocupa el LIMIT: sus sucesores ni siquiera son
     * candidatos, y los de otras claves sí. Un anterior pendiente y listo no excluye: puede ir en el mismo lote (y si
     * no entra, {@link OrdenPorClave} lo resuelve después). El NOT EXISTS usa el índice (clave_kafka, estado, id) y es
     * una lectura sin bloqueo: FOR UPDATE solo bloquea las filas del lote.
     */
    private List<AuditoriaOutboxEvento> reclamar(final int tamano, final Instant ahora, final Instant leaseHasta) {
        final String candidatosSql = "SELECT " + columnas() + " FROM " + tabla + " o"
                + " WHERE o.estado = 'PENDIENTE' AND o.proximo_intento <= ?"
                + " AND NOT EXISTS (SELECT 1 FROM " + tabla + " p WHERE p.clave_kafka = o.clave_kafka AND p.id < o.id"
                + " AND (p.estado IN ('FALLIDO', 'EN_PROCESO') OR (p.estado = 'PENDIENTE' AND p.proximo_intento > ?)))"
                + " ORDER BY o.id LIMIT ? FOR UPDATE SKIP LOCKED";
        final List<AuditoriaOutboxEvento> candidatos = jdbc.query(candidatosSql, mapeo(), ts(ahora), ts(ahora), tamano);
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
        final List<OrdenPorClave.Fila> filas = jdbcConNombres.query("SELECT id, clave_kafka FROM " + tabla
                        + " WHERE clave_kafka IN (:claves) AND estado <> 'ENVIADO' AND id <= :hasta ORDER BY id",
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
        jdbcConNombres.update("UPDATE " + tabla + " SET estado = 'EN_PROCESO', lease_hasta = :lease WHERE id IN (:ids)",
                Map.of("lease", ts(lease), "ids", ids));
        return reclamados;
    }

    @Override
    public List<AuditoriaOutboxEvento> buscarLeasesVencidos(final Instant ahora, final int limite) {
        return jdbc.query("SELECT " + columnas() + " FROM " + tabla
                + " WHERE estado = 'EN_PROCESO' AND lease_hasta <= ? ORDER BY id LIMIT ?", mapeo(), ts(ahora), limite);
    }

    @Override
    public boolean marcarEnviado(final String id, final Instant fechaEnvio) {
        return jdbc.update("UPDATE " + tabla + " SET estado = 'ENVIADO', fecha_envio = ?, lease_hasta = NULL"
                + " WHERE id = ? AND estado <> 'ENVIADO'", ts(fechaEnvio), Long.valueOf(id)) > 0;
    }

    @Override
    public boolean marcarReintento(final AuditoriaOutboxEvento evento, final int intentos,
            final Instant proximoIntento, final String error) {
        return jdbc.update("UPDATE " + tabla + " SET estado = 'PENDIENTE', intentos = ?, proximo_intento = ?, "
                        + "lease_hasta = NULL, ultimo_error = ?" + conLease(), intentos, ts(proximoIntento), error,
                Long.valueOf(evento.id()), ts(evento.leaseHasta())) > 0;
    }

    @Override
    public boolean marcarFallido(final AuditoriaOutboxEvento evento, final int intentos, final String error) {
        return jdbc.update("UPDATE " + tabla + " SET estado = 'FALLIDO', intentos = ?, lease_hasta = NULL, "
                + "ultimo_error = ?" + conLease(), intentos, error, Long.valueOf(evento.id()),
                ts(evento.leaseHasta())) > 0;
    }

    @Override
    public boolean liberar(final AuditoriaOutboxEvento evento) {
        return jdbc.update("UPDATE " + tabla + " SET estado = 'PENDIENTE', lease_hasta = NULL" + conLease(),
                Long.valueOf(evento.id()), ts(evento.leaseHasta())) > 0;
    }

    private static String conLease() {
        return " WHERE id = ? AND estado = 'EN_PROCESO' AND lease_hasta = ?";
    }

    @Override
    public ResumenOutbox resumen() {
        final ResumenOutbox resumen = jdbc.queryForObject("SELECT" + RESUMEN_COLUMNAS + " FROM " + tabla
                + " WHERE estado <> 'ENVIADO'", (rs, fila) -> new ResumenOutbox(rs.getLong("pendientes"),
                rs.getLong("fallidos"), instante(rs, "mas_antiguo")));
        if (resumen == null) {
            return ResumenOutbox.VACIO;
        }
        return resumen;
    }

    @Override
    public Map<String, ResumenTipo> resumenPorTipo() {
        if (!conTipo()) {
            final Map<String, ResumenTipo> soloAuditoria = new LinkedHashMap<>();
            jdbc.query("SELECT" + RESUMEN_COLUMNAS + " FROM " + tabla + " WHERE estado <> 'ENVIADO'",
                    (RowCallbackHandler) rs -> soloAuditoria.put(AuditoriaOutboxEvento.TIPO_AUDITORIA, resumenDe(rs)));
            return soloAuditoria;
        }
        final Map<String, ResumenTipo> porTipo = new LinkedHashMap<>();
        jdbc.query("SELECT COALESCE(tipo, '" + AuditoriaOutboxEvento.TIPO_AUDITORIA + "') AS tipo_metricas,"
                + RESUMEN_COLUMNAS + " FROM " + tabla + " WHERE estado <> 'ENVIADO' GROUP BY tipo_metricas",
                (RowCallbackHandler) rs -> porTipo.put(rs.getString("tipo_metricas"), resumenDe(rs)));
        return porTipo;
    }

    private static ResumenTipo resumenDe(final ResultSet rs) throws SQLException {
        return new ResumenTipo(rs.getLong("pendientes"), rs.getLong("en_proceso"), rs.getLong("fallidos"),
                instante(rs, "mas_antiguo"));
    }

    @Override
    public int limpiarEnviadosAnteriores(final Instant limite, final int maximo) {
        return jdbc.update("DELETE FROM " + tabla + " WHERE estado = 'ENVIADO' AND fecha_envio < ? ORDER BY id LIMIT ?",
                ts(limite), maximo);
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

    private RowMapper<AuditoriaOutboxEvento> mapeo() {
        final boolean leerTipo = conTipo();
        return (rs, fila) -> new AuditoriaOutboxEvento(rs.getString("id"), rs.getString("event_id"),
                rs.getString("clave_kafka"), rs.getString("topic"), rs.getString("payload"),
                EstadoOutbox.valueOf(rs.getString("estado")), rs.getInt("intentos"), instante(rs, "proximo_intento"),
                instante(rs, "lease_hasta"), instante(rs, "fecha_creacion"), instante(rs, "fecha_envio"),
                rs.getString("ultimo_error"), tipoDe(rs, leerTipo));
    }

    private static String tipoDe(final ResultSet rs, final boolean leerTipo) throws SQLException {
        if (leerTipo) {
            return rs.getString("tipo");
        }
        return null;
    }
}
