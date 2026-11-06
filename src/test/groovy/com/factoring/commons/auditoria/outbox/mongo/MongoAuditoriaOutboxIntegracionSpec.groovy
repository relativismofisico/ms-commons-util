package com.factoring.commons.auditoria.outbox.mongo

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.EstadoOutbox
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.OutboxIntegracionBase
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

import java.time.Instant

/** Outbox Mongo contra un replica set real de un nodo (las transacciones de Mongo lo exigen). */
class MongoAuditoriaOutboxIntegracionSpec extends OutboxIntegracionBase {

    static final String COLECCION = "auditoria_outbox_ms_prueba"

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
        cliente = MongoClients.create(mongoDb.getReplicaSetUrl("outbox_it"))
        def fabrica = new SimpleMongoClientDatabaseFactory(cliente, "outbox_it")
        mongo = new MongoTemplate(fabrica)
        adaptador = new MongoAuditoriaOutboxAdapter(mongo, COLECCION)
        adaptador.crearIndices()
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
                d.getInteger("intentos"), d.getDate("proximoIntento")?.toInstant(), d.getDate("leaseHasta")?.toInstant(),
                d.getDate("fechaCreacion")?.toInstant(), d.getDate("fechaEnvio")?.toInstant(), d.getString("ultimoError"))
    }

    @Override
    void forzar(String eventId, EstadoOutbox estado, int intentos, Instant proximoIntento, Instant leaseHasta) {
        mongo.updateFirst(new Query(Criteria.where("eventId").is(eventId)), new Update().set("estado", estado.name())
                .set("intentos", intentos).set("proximoIntento", Date.from(proximoIntento))
                .set("leaseHasta", leaseHasta == null ? null : Date.from(leaseHasta)), COLECCION)
    }

    @Override
    long noEnviados() {
        mongo.count(new Query(Criteria.where("estado").ne("ENVIADO")), COLECCION)
    }

    @Override
    void vaciar() {
        mongo.remove(new Query(), COLECCION)
        mongo.remove(new Query(), "negocio")
    }

    def "crea los índices documentados, con eventId único"() {
        when:
        def indices = mongo.indexOps(COLECCION).indexInfo.collectEntries { [(it.name): it] }

        then:
        indices.keySet().containsAll(["ux_auditoria_outbox_event_id", "ix_auditoria_outbox_lote",
                                      "ix_auditoria_outbox_clave", "ix_auditoria_outbox_limpieza"])
        indices["ux_auditoria_outbox_event_id"].unique
    }

    def "si otro relay reclamó primero un evento, no se toman los siguientes de su clave"() {
        given:
        def a1 = evento("R-A")
        def a2 = evento("R-A")
        def b1 = evento("R-B")
        [a1, a2, b1].each { puerto().registrar(it) }
        MongoTemplate mongoQueRoba = Spy(mongo)
        boolean robado = false
        // entre la lectura de candidatos y el findAndModify, otro relay se lleva a1
        mongoQueRoba.findAndModify(_, _, _, _, _) >> {
            if (!robado) {
                robado = true
                mongo.updateFirst(new Query(Criteria.where("eventId").is(a1.eventId())),
                        new Update().set("estado", "EN_PROCESO"), COLECCION)
            }
            callRealMethod()
        }

        when:
        def lote = new MongoAuditoriaOutboxAdapter(mongoQueRoba, COLECCION)
                .tomarLote(10, reloj.instant(), reloj.instant().plusSeconds(60))

        then:
        lote*.eventId() == [b1.eventId()]
        leer(a2.eventId()).estado() == EstadoOutbox.PENDIENTE
    }
}
