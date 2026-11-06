package com.factoring.commons.auditoria.outbox.jdbc

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.EstadoOutbox
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.OutboxIntegracionBase
import com.zaxxer.hikari.HikariDataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MySQLContainer
import spock.lang.Shared

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** Outbox JDBC contra MySQL 8 real, con la tabla del SQL de referencia del README. */
class JdbcAuditoriaOutboxIntegracionSpec extends OutboxIntegracionBase {

    @Shared
    MySQLContainer<?> mysql = KafkaDePrueba.nombrado(new MySQLContainer<>("mysql:8.0"), "mysql")

    @Shared
    HikariDataSource dataSource

    @Shared
    JdbcTemplate jdbc

    @Shared
    JdbcAuditoriaOutboxAdapter adaptador

    @Shared
    TransactionTemplate transacciones

    def setupSpec() {
        mysql.start()
        // La sesión de MySQL en otra zona que la JVM y que UTC: las fechas no pueden depender de ninguna de las dos.
        dataSource = new HikariDataSource(jdbcUrl: mysql.jdbcUrl, username: mysql.username, password: mysql.password,
                maximumPoolSize: 10, connectionInitSql: "SET time_zone = '+05:00'")
        jdbc = new JdbcTemplate(dataSource)
        jdbc.execute(getClass().getResource("/auditoria_outbox_mysql.sql").text)
        jdbc.execute("CREATE TABLE negocio (id VARCHAR(64) PRIMARY KEY)")
        adaptador = new JdbcAuditoriaOutboxAdapter(dataSource)
        transacciones = new TransactionTemplate(new DataSourceTransactionManager(dataSource))
    }

    def cleanupSpec() {
        dataSource?.close()
        mysql.stop()
    }

    @Override
    AuditoriaOutboxPort outbox() { adaptador }

    @Override
    void enTransaccion(boolean revertir, Closure trabajo) {
        transacciones.executeWithoutResult { estado ->
            trabajo()
            if (revertir) {
                estado.setRollbackOnly()
            }
        }
    }

    @Override
    void escribirNegocio(String id) {
        jdbc.update("INSERT INTO negocio (id) VALUES (?)", id)
    }

    @Override
    boolean existeNegocio(String id) {
        jdbc.queryForObject("SELECT COUNT(*) FROM negocio WHERE id = ?", Long, id) > 0
    }

    @Override
    AuditoriaOutboxEvento leer(String eventId) {
        def filas = jdbc.query("SELECT * FROM auditoria_outbox WHERE event_id = ?", { rs, n ->
            new AuditoriaOutboxEvento(rs.getString("id"), rs.getString("event_id"), rs.getString("clave_kafka"),
                    rs.getString("topic"), rs.getString("payload"), EstadoOutbox.valueOf(rs.getString("estado")),
                    rs.getInt("intentos"), deUtc(rs, "proximo_intento"), deUtc(rs, "lease_hasta"),
                    deUtc(rs, "fecha_creacion"), deUtc(rs, "fecha_envio"), rs.getString("ultimo_error"))
        }, eventId)
        filas ? filas.first() : null
    }

    @Override
    void forzar(String eventId, EstadoOutbox estado, int intentos, Instant proximoIntento, Instant leaseHasta) {
        jdbc.update("UPDATE auditoria_outbox SET estado = ?, intentos = ?, proximo_intento = ?, lease_hasta = ? "
                + "WHERE event_id = ?", estado.name(), intentos, utc(proximoIntento), utc(leaseHasta), eventId)
    }

    private static LocalDateTime utc(Instant instante) {
        instante == null ? null : LocalDateTime.ofInstant(instante, ZoneOffset.UTC)
    }

    /** Valor guardado en la columna, tal cual, sin pasar por el driver como fecha. */
    private String crudo(String eventId, String columna) {
        jdbc.queryForObject("SELECT DATE_FORMAT(" + columna + ", '%Y-%m-%dT%H:%i:%s.%f') FROM auditoria_outbox "
                + "WHERE event_id = ?", String, eventId)
    }

    private static String enUtc(Instant instante) {
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'000'").withZone(ZoneOffset.UTC).format(instante)
    }

    def "UTC: las fechas se guardan en UTC sin importar la zona de la JVM ni la de la sesión de MySQL"() {
        given:
        def e = evento("Z-1")
        def lease = reloj.instant().plus(propiedades.lease)

        when:
        puerto().registrar(e)
        def reclamado = adaptador.tomarLote(10, reloj.instant(), lease).first()
        adaptador.marcarEnviado(reclamado.id(), reloj.instant().plusSeconds(3))
        def leido = leer(e.eventId())

        then:
        TimeZone.default.ID == System.getProperty("user.timezone")
        crudo(e.eventId(), "fecha_creacion") == enUtc(reloj.instant())
        crudo(e.eventId(), "proximo_intento") == enUtc(reloj.instant())
        crudo(e.eventId(), "fecha_envio") == enUtc(reloj.instant().plusSeconds(3))
        reclamado.leaseHasta() == lease
        leido.fechaCreacion() == reloj.instant()
        leido.fechaEnvio() == reloj.instant().plusSeconds(3)
    }

    private static Instant deUtc(java.sql.ResultSet rs, String columna) {
        rs.getObject(columna, LocalDateTime)?.toInstant(ZoneOffset.UTC)
    }

    @Override
    long noEnviados() {
        jdbc.queryForObject("SELECT COUNT(*) FROM auditoria_outbox WHERE estado <> 'ENVIADO'", Long)
    }

    @Override
    void vaciar() {
        jdbc.update("DELETE FROM auditoria_outbox")
        jdbc.update("DELETE FROM negocio")
    }

    def "el reclamo usa SKIP LOCKED: con una fila bloqueada por otra transacción, toma las demás sin esperar"() {
        given:
        def bloqueada = evento("S-1")
        def libre = evento("S-2")
        puerto().registrar(bloqueada)
        puerto().registrar(libre)
        def conexion = dataSource.connection
        conexion.autoCommit = false
        conexion.prepareStatement("SELECT id FROM auditoria_outbox WHERE event_id = '${bloqueada.eventId()}' FOR UPDATE")
                .executeQuery()

        when:
        long inicio = System.currentTimeMillis()
        def lote = adaptador.tomarLote(10, reloj.instant(), reloj.instant().plusSeconds(60))

        then:
        lote*.eventId() == [libre.eventId()]
        System.currentTimeMillis() - inicio < 5_000

        cleanup:
        conexion?.rollback()
        conexion?.close()
    }
}
