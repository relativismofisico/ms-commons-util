package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.zaxxer.hikari.HikariDataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MySQLContainer
import spock.lang.Shared

/** 0.6.1 sobre una tabla propia del servicio (esquema 0.6.0) en MySQL 8 real. */
class JdbcEventIdPropioIntegracionSpec extends EventIdPropioBaseSpec {

    static final String TABLA = "outbox_ms_prueba"

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
        jdbc.execute(getClass().getResource("/outbox_mysql.sql").text)
        jdbc.execute("CREATE TABLE negocio (id VARCHAR(64) PRIMARY KEY)")
        adaptador = new JdbcAuditoriaOutboxAdapter(dataSource, TABLA)
        transacciones = new TransactionTemplate(new DataSourceTransactionManager(dataSource))
    }

    def cleanupSpec() {
        dataSource?.close()
        mysql.stop()
    }

    @Override
    AuditoriaOutboxPort almacen() { adaptador }

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
        jdbc.queryForObject("SELECT COUNT(*) FROM negocio WHERE id = ?", Integer, id) > 0
    }

    @Override
    List<String> eventIdsGuardados() {
        jdbc.queryForList("SELECT event_id FROM " + TABLA + " ORDER BY id", String)
    }

    @Override
    String sobreGuardado(String eventId) {
        jdbc.queryForObject("SELECT payload FROM " + TABLA + " WHERE event_id = ?", String, eventId)
    }

    @Override
    void vaciar() {
        jdbc.update("DELETE FROM " + TABLA)
        jdbc.update("DELETE FROM negocio")
    }
}
