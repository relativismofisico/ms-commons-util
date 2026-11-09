package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.mongo.MongoAuditoriaOutboxAdapter
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import org.bson.Document
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.MongoDBContainer
import spock.lang.Shared

import java.time.Duration

/**
 * 0.6.1 sobre una colección propia del servicio en un replica set real de Mongo 7. En Mongo un error de clave duplicada
 * aborta la transacción entera: aquí se comprueba que el duplicado no la toca.
 */
class MongoEventIdPropioIntegracionSpec extends EventIdPropioBaseSpec {

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
        cliente = MongoClients.create(mongoDb.getReplicaSetUrl("outbox_eventid_it"))
        def fabrica = new SimpleMongoClientDatabaseFactory(cliente, "outbox_eventid_it")
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
    List<String> eventIdsGuardados() {
        mongo.find(new Query().with(Sort.by("_id")), Document, COLECCION)*.getString("eventId")
    }

    @Override
    String sobreGuardado(String eventId) {
        mongo.findOne(new Query(Criteria.where("eventId").is(eventId)), Document, COLECCION).getString("payload")
    }

    @Override
    void vaciar() {
        mongo.remove(new Query(), COLECCION)
        mongo.remove(new Query(), "negocio")
    }

    def "el documento guardado con eventId propio tiene los mismos campos que uno guardado con insert"() {
        given:
        def r = registro()
        r.registrar(TIPO_SOBRE, documento("U"))
        r.registrar(TIPO_SOBRE, "ID-U", documento("U"))

        when:
        def docs = mongo.find(new Query().with(Sort.by("_id")), Document, COLECCION)

        then:
        docs.size() == 2
        docs[0].keySet() == docs[1].keySet()
        docs[1].getString("estado") == "PENDIENTE"
        docs[1].getInteger("intentos") == 0
        docs[1].getString("tipo") == TIPO_SOBRE
        docs[1].get("proximoIntento") instanceof Date
    }
}
