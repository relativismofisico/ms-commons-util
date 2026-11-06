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
        dataSource = new HikariDataSource(jdbcUrl: mysql.jdbcUrl, username: mysql.username, password: mysql.password,
                maximumPoolSize: 10)
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
                    rs.getInt("intentos"), rs.getTimestamp("proximo_intento")?.toInstant(),
                    rs.getTimestamp("lease_hasta")?.toInstant(), rs.getTimestamp("fecha_creacion")?.toInstant(),
                    rs.getTimestamp("fecha_envio")?.toInstant(), rs.getString("ultimo_error"))
        }, eventId)
        filas ? filas.first() : null
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
