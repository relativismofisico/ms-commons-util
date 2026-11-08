package com.factoring.commons.auditoria.outbox

import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.factoring.commons.auditoria.outbox.mongo.MongoAuditoriaOutboxAdapter
import com.mongodb.client.MongoClients
import com.zaxxer.hikari.HikariDataSource
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Medición de {@code tomarLote} con 100 000 eventos en 2 000 claves de 50, donde 40 claves tienen un FALLIDO en cabeza
 * y 20 su primer evento en backoff, y esas 60 claves van primero en el orden de creación (el escenario de la issue
 * #14). No corre en {@code check} ni en CI: se lanza a mano con {@code ./gradlew medirTomarLote} contra un MySQL 8 y un
 * Mongo 7 (replica set) locales, indicados con las propiedades {@code -Dmedicion.*} (nunca se imprimen). Usa una base
 * propia que crea y borra. Solo usa la API de 0.5.1, para poder medir antes y después con el mismo código.
 */
class MedicionTomarLote extends Specification {

    static final int CLAVES = 2000
    static final int POR_CLAVE = 50
    static final int FALLIDAS = 40
    static final int EN_BACKOFF = 20
    static final int CICLOS = 20
    static final int LOTE = 50
    static final String BASE = "medicion_outbox"
    static final String PAYLOAD = '{"detalle":"' + ("x" * 600) + '"}'

    def "JDBC (MySQL)"() {
        given:
        def raiz = new HikariDataSource(jdbcUrl: System.getProperty("medicion.jdbc.url"),
                username: System.getProperty("medicion.jdbc.usuario"), password: System.getProperty("medicion.jdbc.clave"))
        raiz.connection.withCloseable { c -> c.createStatement().execute("CREATE DATABASE IF NOT EXISTS " + BASE) }
        def ds = new HikariDataSource(jdbcUrl: System.getProperty("medicion.jdbc.url").replaceFirst('/[^/?]*(\\?|$)', '/' + BASE + '$1'),
                username: System.getProperty("medicion.jdbc.usuario"), password: System.getProperty("medicion.jdbc.clave"))
        def jdbc = new org.springframework.jdbc.core.JdbcTemplate(ds)
        jdbc.execute("DROP TABLE IF EXISTS auditoria_outbox")
        jdbc.execute(getClass().getResource("/auditoria_outbox_mysql.sql").text)
        def ahora = Instant.now()
        sembrarJdbc(jdbc, ahora)
        def adaptador = new JdbcAuditoriaOutboxAdapter(ds)

        when:
        def medidas = medir(adaptador, ahora)

        then:
        reportar("jdbc", medidas)

        cleanup:
        ds?.close()
        raiz?.connection?.withCloseable { c -> c.createStatement().execute("DROP DATABASE IF EXISTS " + BASE) }
        raiz?.close()
    }

    def "Mongo"() {
        given:
        def cliente = MongoClients.create(System.getProperty("medicion.mongo.uri"))
        def mongo = new MongoTemplate(cliente, BASE)
        mongo.db.drop()
        def adaptador = new MongoAuditoriaOutboxAdapter(mongo, "auditoria_outbox_medicion")
        adaptador.crearIndices()
        def ahora = Instant.now()
        sembrarMongo(mongo, "auditoria_outbox_medicion", ahora)

        when:
        def medidas = medir(adaptador, ahora)

        then:
        reportar("mongo", medidas)

        cleanup:
        mongo?.db?.drop()
        cliente?.close()
    }

    /** Estado del evento {@code n} (0-based) de la clave {@code k}: las primeras 60 claves bloqueadas en cabeza. */
    static Map fila(int k, int n, Instant ahora, long orden) {
        def estado = "PENDIENTE"
        def proximo = ahora.minusSeconds(60)
        if (n == 0 && k < FALLIDAS) {
            estado = "FALLIDO"
        } else if (n == 0 && k < FALLIDAS + EN_BACKOFF) {
            proximo = ahora.plus(Duration.ofHours(1))
        }
        [eventId: UUID.randomUUID().toString(), clave: "C:" + k, estado: estado, proximo: proximo,
         creacion: ahora.minus(Duration.ofDays(1)).plusMillis(orden)]
    }

    static void sembrarJdbc(def jdbc, Instant ahora) {
        def sql = "INSERT INTO auditoria_outbox (event_id, clave_kafka, topic, payload, estado, intentos, " +
                "proximo_intento, fecha_creacion) VALUES (?, ?, 'medicion', ?, ?, 0, ?, ?)"
        long orden = 0
        for (int k = 0; k < CLAVES; k++) {
            def filas = (0..<POR_CLAVE).collect { n ->
                def f = fila(k, n, ahora, orden++)
                [f.eventId, f.clave, PAYLOAD, f.estado, LocalDateTime.ofInstant(f.proximo, ZoneOffset.UTC),
                 LocalDateTime.ofInstant(f.creacion, ZoneOffset.UTC)] as Object[]
            }
            jdbc.batchUpdate(sql, filas)
        }
    }

    static void sembrarMongo(MongoTemplate mongo, String coleccion, Instant ahora) {
        long orden = 0
        for (int k = 0; k < CLAVES; k++) {
            def documentos = (0..<POR_CLAVE).collect { n ->
                def f = fila(k, n, ahora, orden++)
                new Document("eventId", f.eventId).append("claveKafka", f.clave).append("topic", "medicion")
                        .append("payload", PAYLOAD).append("estado", f.estado).append("intentos", 0)
                        .append("proximoIntento", Date.from(f.proximo)).append("leaseHasta", null)
                        .append("fechaCreacion", Date.from(f.creacion)).append("fechaEnvio", null)
                        .append("ultimoError", null)
            }
            mongo.getCollection(coleccion).insertMany(documentos)
        }
    }

    static List<Map> medir(AuditoriaOutboxPort adaptador, Instant ahora) {
        (1..CICLOS).collect {
            long inicio = System.nanoTime()
            def lote = adaptador.tomarLote(LOTE, ahora, ahora.plus(Duration.ofMinutes(5)))
            long ms = Math.round((System.nanoTime() - inicio) / 1_000_000d)
            def bloqueados = lote.count { (it.claveKafka().substring(2) as int) < FALLIDAS + EN_BACKOFF }
            [ms: ms, tomados: lote.size(), bloqueados: bloqueados]
        }
    }

    static boolean reportar(String almacen, List<Map> medidas) {
        def tiempos = medidas*.ms.sort()
        def p95 = tiempos[(int) Math.ceil(0.95 * tiempos.size()) - 1]
        println "MEDICION almacen=${almacen} ciclos(ms)=${medidas*.ms} tomados=${medidas*.tomados as Set} " +
                "bloqueados=${medidas*.bloqueados.sum()} p95=${p95} max=${tiempos.last()} " +
                "mediana=${tiempos[(int) (tiempos.size() / 2)]}"
        medidas.every { it.tomados == LOTE && it.bloqueados == 0 }
    }
}
