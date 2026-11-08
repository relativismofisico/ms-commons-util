package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.EstadoOutbox
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.OutboxAuditoriaPort
import com.factoring.commons.auditoria.outbox.OutboxIntegracionBase
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.zaxxer.hikari.HikariDataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MySQLContainer
import spock.lang.Shared

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/** Outbox genérico sobre una tabla propia del servicio ({@code outbox_ms_prueba}, esquema 0.6.0) en MySQL 8 real. */
class JdbcOutboxGenericoIntegracionSpec extends OutboxGenericoBaseSpec {

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
                maximumPoolSize: 10, connectionInitSql: "SET time_zone = '+05:00'")
        jdbc = new JdbcTemplate(dataSource)
        jdbc.execute(getClass().getResource("/outbox_mysql.sql").text)
        jdbc.execute(getClass().getResource("/auditoria_outbox_mysql.sql").text)
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
    AuditoriaOutboxEvento leer(String eventId) {
        def filas = jdbc.queryForList("SELECT * FROM " + TABLA + " WHERE event_id = ?", eventId)
        if (filas.isEmpty()) {
            return null
        }
        def f = filas[0]
        new AuditoriaOutboxEvento(f.id as String, f.event_id, f.clave_kafka, f.topic, f.payload,
                EstadoOutbox.valueOf(f.estado as String), f.intentos as int, null, null, null, null, f.ultimo_error,
                f.tipo as String)
    }

    @Override
    void forzar(String eventId, EstadoOutbox estado, Instant proximoIntento) {
        jdbc.update("UPDATE " + TABLA + " SET estado = ?, proximo_intento = ? WHERE event_id = ?", estado.name(),
                LocalDateTime.ofInstant(proximoIntento, ZoneOffset.UTC), eventId)
    }

    @Override
    void vaciar() {
        jdbc.update("DELETE FROM " + TABLA)
        jdbc.update("DELETE FROM auditoria_outbox")
        jdbc.update("DELETE FROM negocio")
    }

    def "tabla de 0.5.x sin la columna tipo: los eventos genéricos fallan con un mensaje claro y la auditoría sigue igual"() {
        given:
        def legado = new JdbcAuditoriaOutboxAdapter(dataSource)
        def accion = OutboxIntegracionBase.evento("L-1")

        when:
        legado.verificarEventosGenericos()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("auditoria_outbox") && e.message.contains("tipo")

        when:
        new OutboxAuditoriaPort(legado, "auditoria.accion", "ms-prueba", reloj).registrar(accion)

        then:
        legado.resumen().pendientes() == 1
        legado.resumenPorTipo().keySet() == [AuditoriaOutboxEvento.TIPO_AUDITORIA] as Set
        legado.tomarLote(10, reloj.instant(), reloj.instant().plusSeconds(60))*.eventId() == [accion.eventId()]
    }

    def "la tabla configurada es la que se usa, y un nombre inválido se rechaza al construir"() {
        expect:
        adaptador.tabla == TABLA
        new JdbcAuditoriaOutboxAdapter(dataSource).tabla == "auditoria_outbox"

        when:
        new JdbcAuditoriaOutboxAdapter(dataSource, nombre)

        then:
        thrown(IllegalArgumentException)

        where:
        nombre << [null, "", "outbox; DROP TABLE negocio", "1tabla", "a" * 65]
    }
}
