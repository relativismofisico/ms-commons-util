package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.EstadoOutbox
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.mongo.MongoAuditoriaOutboxAdapter
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import org.bson.Document
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MongoDBContainer
import spock.lang.Shared

import java.time.Duration
import java.time.Instant

/** Outbox genérico sobre una colección propia del servicio en un replica set real de Mongo 7. */
class MongoOutboxGenericoIntegracionSpec extends OutboxGenericoBaseSpec {

    static final String COLECCION = "outbox_ms_prueba"

    @Shared
    MongoDBContainer mongoDb = KafkaDePrueba.nombrado(new MongoDBContainer("mongo:7.0"), "mongo")

    @Shared
    MongoClient cliente

    @Shared
    MongoTemplate mongo

    @Shared
    MongoAuditoriaOutboxAdapter adaptador

    @Shared
    TransactionTemplate transacciones

    def setupSpec() {
        mongoDb.start()
        cliente = MongoClients.create(mongoDb.getReplicaSetUrl("outbox_generico_it"))
        def fabrica = new SimpleMongoClientDatabaseFactory(cliente, "outbox_generico_it")
        mongo = new MongoTemplate(fabrica)
        adaptador = new MongoAuditoriaOutboxAdapter(mongo, COLECCION, Duration.ofDays(30))
        adaptador.crearIndices()
        if (!mongo.collectionExists("negocio")) {
            mongo.createCollection("negocio")
        }
        transacciones = new TransactionTemplate(new MongoTransactionManager(fabrica))
    }

    def cleanupSpec() {
        cliente?.close()
        mongoDb.stop()
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
        mongo.insert(new Document("_id", id), "negocio")
    }

    @Override
    boolean existeNegocio(String id) {
        mongo.exists(new Query(Criteria.where("_id").is(id)), "negocio")
    }

    @Override
    AuditoriaOutboxEvento leer(String eventId) {
        def d = mongo.findOne(new Query(Criteria.where("eventId").is(eventId)), Document, COLECCION)
        if (d == null) {
            return null
        }
        new AuditoriaOutboxEvento(d.getObjectId("_id").toHexString(), d.getString("eventId"), d.getString("claveKafka"),
                d.getString("topic"), d.getString("payload"), EstadoOutbox.valueOf(d.getString("estado")),
                d.getInteger("intentos"), null, null, null, null, d.getString("ultimoError"), d.getString("tipo"))
    }

    @Override
    void forzar(String eventId, EstadoOutbox estado, Instant proximoIntento) {
        mongo.updateFirst(new Query(Criteria.where("eventId").is(eventId)), new Update().set("estado", estado.name())
                .set("proximoIntento", Date.from(proximoIntento)), COLECCION)
    }

    @Override
    void vaciar() {
        mongo.remove(new Query(), COLECCION)
        mongo.remove(new Query(), "negocio")
    }

    def "índices: el del orden de candidatos y el TTL parcial de los ENVIADO con la retención; cambiar la retención lo ajusta"() {
        when:
        def indices = mongo.indexOps(COLECCION).indexInfo.collectEntries { [(it.name): it] }

        then:
        indices.keySet().containsAll(["ix_outbox_orden", "ttl_outbox_enviados", "ix_auditoria_outbox_lote",
                                      "ix_auditoria_outbox_clave"])
        ttl() == Duration.ofDays(30).toSeconds()

        when:
        new MongoAuditoriaOutboxAdapter(mongo, COLECCION, Duration.ofDays(7)).crearIndices()

        then:
        ttl() == Duration.ofDays(7).toSeconds()

        cleanup:
        adaptador.crearIndices()
    }

    private long ttl() {
        def indice = mongo.getCollection(COLECCION).listIndexes().find { it.getString("name") == "ttl_outbox_enviados" }
        assert indice.get("partialFilterExpression") == new Document("estado", "ENVIADO")
        (indice.get("expireAfterSeconds") as Number).longValue()
    }
}
