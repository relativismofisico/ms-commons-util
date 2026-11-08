package com.factoring.commons.outbox.idempotencia

import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.mongodb.MongoException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import org.bson.Document
import org.springframework.dao.DataAccessException
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.testcontainers.containers.MongoDBContainer
import spock.lang.Shared

import java.time.Clock
import java.time.Duration
import java.time.Instant

/** Consumo idempotente Mongo contra un replica set real de Mongo 7 (las transacciones lo exigen). */
class MongoConsumoIdempotenteIntegracionSpec extends ConsumoIdempotenteBaseSpec {

    @Shared
    MongoDBContainer mongoDb = KafkaDePrueba.nombrado(new MongoDBContainer("mongo:7.0"), "mongo")

    @Shared
    MongoClient cliente

    @Shared
    MongoTemplate mongo

    @Shared
    MongoConsumoIdempotente adaptador

    def setupSpec() {
        mongoDb.start()
        cliente = MongoClients.create(mongoDb.getReplicaSetUrl("consumo_it"))
        def fabrica = new SimpleMongoClientDatabaseFactory(cliente, "consumo_it")
        mongo = new MongoTemplate(fabrica)
        adaptador = new MongoConsumoIdempotente(mongo, new MongoTransactionManager(fabrica),
                MongoConsumoIdempotente.COLECCION, Duration.ofDays(30), Clock.systemUTC())
        adaptador.crearIndices()
        adaptador.crearIndices()
        if (!mongo.collectionExists("negocio")) {
            mongo.createCollection("negocio")
        }
    }

    def cleanupSpec() {
        cliente?.close()
        mongoDb.stop()
    }

    @Override
    ConsumoIdempotente consumo() { adaptador }

    @Override
    void escribirNegocio(String id) {
        mongo.insert(new Document("id", id), "negocio")
    }

    @Override
    long negocios(String id) {
        mongo.count(new Query(Criteria.where("id").is(id)), "negocio")
    }

    @Override
    long marcas() {
        mongo.count(new Query(), MongoConsumoIdempotente.COLECCION)
    }

    @Override
    void marcaAntigua(String consumidor, String clave, Instant fecha) {
        mongo.insert(new Document("_id", new Document("consumidor", consumidor).append("clave", clave))
                .append("fechaProcesado", Date.from(fecha)), MongoConsumoIdempotente.COLECCION)
    }

    @Override
    void vaciar() {
        mongo.remove(new Query(), MongoConsumoIdempotente.COLECCION)
        mongo.remove(new Query(), "negocio")
    }

    @Override
    boolean esTransitorio(Throwable error) {
        // Dos transacciones que insertan la misma marca: una recibe un conflicto de escritura transitorio
        for (Throwable causa = error; causa != null; causa = causa.cause) {
            if (causa instanceof MongoException && ((MongoException) causa).hasErrorLabel("TransientTransactionError")) {
                return true
            }
        }
        error instanceof DataAccessException && error.message?.contains("WriteConflict")
    }

    def "el índice TTL aplica la retención y se ajusta si cambia"() {
        expect:
        ttl() == Duration.ofDays(30).toSeconds()

        when:
        new MongoConsumoIdempotente(mongo, new MongoTransactionManager(mongo.mongoDatabaseFactory),
                MongoConsumoIdempotente.COLECCION, Duration.ofDays(45), Clock.systemUTC()).crearIndices()

        then:
        ttl() == Duration.ofDays(45).toSeconds()

        cleanup:
        adaptador.crearIndices()
    }

    private long ttl() {
        def indice = mongo.getCollection(MongoConsumoIdempotente.COLECCION).listIndexes()
                .find { it.getString("name") == "ttl_evento_procesado" }
        (indice.get("expireAfterSeconds") as Number).longValue()
    }
}
