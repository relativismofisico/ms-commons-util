package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.zaxxer.hikari.HikariDataSource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MySQLContainer
import spock.lang.Shared

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

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

    /**
     * Transacciones de InnoDB esperando un bloqueo (la segunda, sobre el índice único de event_id). Se consulta como
     * root del contenedor: el usuario de la prueba no tiene el privilegio PROCESS.
     */
    private int esperandoBloqueo() {
        new JdbcTemplate(new SingleConnectionDataSource(mysql.jdbcUrl, "root", mysql.password, true)).queryForObject("SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'", Integer)
    }

    def "concurrencia: la segunda transacción espera el bloqueo de la primera y, si confirma, termina sin segundo evento (#desenlace)"() {
        given:
        def r = registro()
        def primeraEscribio = new CountDownLatch(1)
        def terminarPrimera = new CountDownLatch(1)
        def segunda = new CompletableFuture<Optional>()

        when: "la primera registra y queda abierta"
        def hilo1 = Thread.start {
            enTransaccion(!confirmaPrimera) {
                escribirNegocio("C-1")
                r.registrar(TIPO_ALIAS, "ID-CONC", documento("1", "primera"))
                primeraEscribio.countDown()
                terminarPrimera.await(30, TimeUnit.SECONDS)
            }
        }
        primeraEscribio.await(30, TimeUnit.SECONDS)
        and: "la segunda registra el mismo eventId y se bloquea"
        def hilo2 = Thread.start {
            try {
                Optional resultado = null
                enTransaccion(false) {
                    escribirNegocio("C-2")
                    resultado = r.registrar(TIPO_ALIAS, "ID-CONC", documento("1", "segunda"))
                }
                segunda.complete(resultado)
            } catch (Throwable t) {
                segunda.completeExceptionally(t)
            }
        }
        def hasta = System.currentTimeMillis() + 10_000
        while (esperandoBloqueo() == 0 && System.currentTimeMillis() < hasta) {
            Thread.sleep(20)
        }
        def bloqueada = esperandoBloqueo() == 1 && !segunda.isDone()
        and: "la primera termina"
        terminarPrimera.countDown()
        def resultado = segunda.get(30, TimeUnit.SECONDS)
        [hilo1, hilo2]*.join(10_000)

        then:
        bloqueada
        resultado.isPresent() == !confirmaPrimera
        eventIdsGuardados() == ["ID-CONC"]
        OutboxJson.deserializar(sobreGuardado("ID-CONC"), DocumentoEmitido).payload().titulo() == tituloGuardado
        existeNegocio("C-2")
        existeNegocio("C-1") == confirmaPrimera
        r.duplicados(TIPO_ALIAS) == (confirmaPrimera ? 1 : 0)

        where:
        desenlace                     | confirmaPrimera || tituloGuardado
        "la primera confirma"         | true            || "primera"
        "la primera se revierte"      | false           || "segunda"
    }
}
