package com.factoring.commons.outbox.idempotencia

import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.zaxxer.hikari.HikariDataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.testcontainers.containers.MySQLContainer
import spock.lang.Shared

import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/** Consumo idempotente JDBC contra MySQL 8 real, con la tabla del SQL de referencia del README. */
class JdbcConsumoIdempotenteIntegracionSpec extends ConsumoIdempotenteBaseSpec {

    @Shared
    MySQLContainer<?> mysql = KafkaDePrueba.nombrado(new MySQLContainer<>("mysql:8.0"), "mysql")

    @Shared
    HikariDataSource dataSource

    @Shared
    JdbcTemplate jdbc

    @Shared
    JdbcConsumoIdempotente adaptador

    def setupSpec() {
        mysql.start()
        dataSource = new HikariDataSource(jdbcUrl: mysql.jdbcUrl, username: mysql.username, password: mysql.password,
                maximumPoolSize: 12, connectionInitSql: "SET time_zone = '+05:00'")
        jdbc = new JdbcTemplate(dataSource)
        jdbc.execute(getClass().getResource("/evento_procesado_mysql.sql").text)
        jdbc.execute("CREATE TABLE negocio (id VARCHAR(64) NOT NULL, n BIGINT AUTO_INCREMENT PRIMARY KEY)")
        adaptador = new JdbcConsumoIdempotente(dataSource, new DataSourceTransactionManager(dataSource),
                JdbcConsumoIdempotente.TABLA, Clock.systemUTC())
    }

    def cleanupSpec() {
        dataSource?.close()
        mysql.stop()
    }

    @Override
    ConsumoIdempotente consumo() { adaptador }

    @Override
    void escribirNegocio(String id) {
        jdbc.update("INSERT INTO negocio (id) VALUES (?)", id)
    }

    @Override
    long negocios(String id) {
        jdbc.queryForObject("SELECT COUNT(*) FROM negocio WHERE id = ?", Long, id)
    }

    @Override
    long marcas() {
        jdbc.queryForObject("SELECT COUNT(*) FROM evento_procesado", Long)
    }

    @Override
    void marcaAntigua(String consumidor, String clave, Instant fecha) {
        jdbc.update("INSERT INTO evento_procesado (consumidor, clave, fecha_procesado) VALUES (?, ?, ?)", consumidor,
                clave, LocalDateTime.ofInstant(fecha, ZoneOffset.UTC))
    }

    @Override
    void vaciar() {
        jdbc.update("DELETE FROM evento_procesado")
        jdbc.update("DELETE FROM negocio")
    }

    @Override
    boolean esTransitorio(Throwable error) {
        // En MySQL el duplicado concurrente espera el bloqueo y termina como duplicado: no hay fallos transitorios
        false
    }

    def "nombre de tabla inválido: se rechaza al construir"() {
        when:
        new JdbcConsumoIdempotente(dataSource, new DataSourceTransactionManager(dataSource), nombre, Clock.systemUTC())

        then:
        thrown(IllegalArgumentException)

        where:
        nombre << [null, "evento procesado", "x;DROP TABLE negocio"]
    }
}
