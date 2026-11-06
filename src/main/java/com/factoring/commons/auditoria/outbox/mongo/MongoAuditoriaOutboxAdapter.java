package com.factoring.commons.auditoria.outbox.mongo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento;
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort;
import com.factoring.commons.auditoria.outbox.EstadoOutbox;
import com.factoring.commons.auditoria.outbox.OrdenPorClave;
import com.factoring.commons.auditoria.outbox.ResumenOutbox;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * {@link AuditoriaOutboxPort} sobre una colección propia del servicio en su MongoDB.
 *
 * <p>{@link #guardar} usa el {@code MongoTemplate} del servicio, así participa en la transacción activa de un
 * {@code MongoTransactionManager} (requiere replica set). Sin transacción de Mongo el insert se confirma solo, en el
 * momento. El reclamo se hace documento a documento con {@code findAndModify} condicionado a {@code PENDIENTE}: dos
 * relays nunca reclaman el mismo evento.</p>
 *
 * <p>Índices (los crea {@link #crearIndices()} al arrancar; ver el README):
 * {@code eventId} único; {@code {estado, proximoIntento, fechaCreacion, _id}} para el lote;
 * {@code {claveKafka, estado, fechaCreacion}} para el orden por clave; {@code {estado, fechaEnvio}} para la
 * limpieza.</p>
 */
public class MongoAuditoriaOutboxAdapter implements AuditoriaOutboxPort {

    private static final String ID = "_id";
    private static final String EVENT_ID = "eventId";
    private static final String CLAVE = "claveKafka";
    private static final String TOPIC = "topic";
    private static final String PAYLOAD = "payload";
    private static final String ESTADO = "estado";
    private static final String INTENTOS = "intentos";
    private static final String PROXIMO = "proximoIntento";
    private static final String LEASE = "leaseHasta";
    private static final String CREACION = "fechaCreacion";
    private static final String ENVIO = "fechaEnvio";
    private static final String ERROR = "ultimoError";
    private static final String BLOQUEOS = "bloqueos";

    private static final Sort ORDEN = Sort.by(Sort.Direction.ASC, CREACION).and(Sort.by(Sort.Direction.ASC, ID));

    private final MongoOperations mongo;
    private final String coleccion;

    /**
     * @param mongo     el {@code MongoTemplate} del servicio
     * @param coleccion colección del outbox, exclusiva del servicio
     */
    public MongoAuditoriaOutboxAdapter(final MongoOperations mongo, final String coleccion) {
        this.mongo = mongo;
        this.coleccion = coleccion;
    }

    /**
     * Colección del outbox.
     *
     * @return el nombre
     */
    public String getColeccion() {
        return coleccion;
    }

    /** Crea la colección (si no existe) y sus índices; es idempotente. */
    public void crearIndices() {
        if (!mongo.collectionExists(coleccion)) {
            mongo.createCollection(coleccion);
        }
        final IndexOperations indices = mongo.indexOps(coleccion);
        indices.createIndex(new Index().on(EVENT_ID, Sort.Direction.ASC).unique().named("ux_auditoria_outbox_event_id"));
        indices.createIndex(new CompoundIndexDefinition(new Document(ESTADO, 1).append(PROXIMO, 1).append(CREACION, 1)
                .append(ID, 1)).named("ix_auditoria_outbox_lote"));
        indices.createIndex(new CompoundIndexDefinition(new Document(CLAVE, 1).append(ESTADO, 1).append(CREACION, 1))
                .named("ix_auditoria_outbox_clave"));
        indices.createIndex(new CompoundIndexDefinition(new Document(ESTADO, 1).append(ENVIO, 1))
                .named("ix_auditoria_outbox_limpieza"));
    }

    @Override
    public void guardar(final AuditoriaOutboxEvento evento) {
        final Document documento = new Document(EVENT_ID, evento.eventId())
                .append(CLAVE, evento.claveKafka())
                .append(TOPIC, evento.topic())
                .append(PAYLOAD, evento.payload())
                .append(ESTADO, evento.estado().name())
                .append(INTENTOS, evento.intentos())
                .append(PROXIMO, fecha(evento.proximoIntento()))
                .append(LEASE, null)
                .append(CREACION, fecha(evento.fechaCreacion()))
                .append(ENVIO, null)
                .append(ERROR, null);
        mongo.insert(documento, coleccion);
    }

    @Override
    public List<AuditoriaOutboxEvento> tomarLote(final int tamano, final Instant ahora, final Instant leaseHasta) {
        final List<Document> candidatos = candidatos(tamano, fecha(ahora));
        if (candidatos.isEmpty()) {
            return List.of();
        }
        final Set<String> idsCandidatos = new HashSet<>();
        final Set<String> claves = new LinkedHashSet<>();
        for (final Document candidato : candidatos) {
            idsCandidatos.add(candidato.getObjectId(ID).toHexString());
            claves.add(candidato.getString(CLAVE));
        }
        final Date ultimaCreacion = candidatos.get(candidatos.size() - 1).getDate(CREACION);
        final Query noEnviados = new Query(Criteria.where(CLAVE).in(claves)
                .and(ESTADO).ne(EstadoOutbox.ENVIADO.name())
                .and(CREACION).lte(ultimaCreacion)).with(ORDEN);
        noEnviados.fields().include(ID, CLAVE);
        final List<OrdenPorClave.Fila> filas = new ArrayList<>();
        for (final Document fila : mongo.find(noEnviados, Document.class, coleccion)) {
            final String id = fila.getObjectId(ID).toHexString();
            filas.add(new OrdenPorClave.Fila(id, fila.getString(CLAVE), idsCandidatos.contains(id)));
        }
        final Set<String> elegibles = OrdenPorClave.elegibles(filas);
        final Date lease = fecha(leaseHasta);
        final Set<String> clavesDetenidas = new HashSet<>();
        final List<AuditoriaOutboxEvento> reclamados = new ArrayList<>();
        for (final Document candidato : candidatos) {
            final String clave = candidato.getString(CLAVE);
            if (!elegibles.contains(candidato.getObjectId(ID).toHexString()) || clavesDetenidas.contains(clave)) {
                continue;
            }
            final Document reclamado = mongo.findAndModify(
                    new Query(Criteria.where(ID).is(candidato.getObjectId(ID))
                            .and(ESTADO).is(EstadoOutbox.PENDIENTE.name())),
                    new Update().set(ESTADO, EstadoOutbox.EN_PROCESO.name()).set(LEASE, lease),
                    FindAndModifyOptions.options().returnNew(true), Document.class, coleccion);
            if (reclamado == null) {
                // Otro relay lo tomó: los siguientes de esta clave tampoco son nuestros.
                clavesDetenidas.add(clave);
            } else {
                reclamados.add(mapear(reclamado));
            }
        }
        return reclamados;
    }

    /**
     * Pendientes listos sin un anterior de su clave que NO pueda ir en este lote (FALLIDO, EN_PROCESO o PENDIENTE en
     * backoff): el equivalente del NOT EXISTS del almacén JDBC, con un {@code $lookup} sobre la misma colección que
     * usa el índice {@code {claveKafka, estado, fechaCreacion}}. Sin esto, los sucesores de una clave bloqueada
     * ocupaban todo el {@code limit} y las demás claves no salían nunca.
     */
    private List<Document> candidatos(final int tamano, final Date ahora) {
        final Document anterior = new Document("$or", List.of(
                new Document("$lt", List.of("$" + CREACION, "$$creacion")),
                new Document("$and", List.of(new Document("$eq", List.of("$" + CREACION, "$$creacion")),
                        new Document("$lt", List.of("$" + ID, "$$id"))))));
        final Document bloquea = new Document("$or", List.of(
                new Document("$in", List.of("$" + ESTADO,
                        List.of(EstadoOutbox.FALLIDO.name(), EstadoOutbox.EN_PROCESO.name()))),
                new Document("$and", List.of(new Document("$eq", List.of("$" + ESTADO, EstadoOutbox.PENDIENTE.name())),
                        new Document("$gt", List.of("$" + PROXIMO, ahora))))));
        final Document bloqueos = new Document("from", coleccion)
                .append("let", new Document("clave", "$" + CLAVE).append("creacion", "$" + CREACION).append("id", "$" + ID))
                .append("pipeline", List.of(
                        new Document("$match", new Document("$expr", new Document("$and", List.of(
                                new Document("$eq", List.of("$" + CLAVE, "$$clave")), anterior, bloquea)))),
                        new Document("$limit", 1),
                        new Document("$project", new Document(ID, 1))))
                .append("as", BLOQUEOS);
        final List<Document> etapas = List.of(
                new Document("$match", new Document(ESTADO, EstadoOutbox.PENDIENTE.name())
                        .append(PROXIMO, new Document("$lte", ahora))),
                new Document("$sort", new Document(CREACION, 1).append(ID, 1)),
                new Document("$lookup", bloqueos),
                new Document("$match", new Document(BLOQUEOS, new Document("$size", 0))),
                new Document("$limit", tamano),
                new Document("$project", new Document(BLOQUEOS, 0)));
        final List<AggregationOperation> operaciones = new ArrayList<>();
        for (final Document etapa : etapas) {
            operaciones.add(contexto -> etapa);
        }
        return mongo.aggregate(Aggregation.newAggregation(operaciones), coleccion, Document.class).getMappedResults();
    }

    @Override
    public List<AuditoriaOutboxEvento> buscarLeasesVencidos(final Instant ahora, final int limite) {
        final Query vencidos = new Query(Criteria.where(ESTADO).is(EstadoOutbox.EN_PROCESO.name())
                .and(LEASE).lte(fecha(ahora))).with(ORDEN).limit(limite);
        final List<AuditoriaOutboxEvento> eventos = new ArrayList<>();
        for (final Document documento : mongo.find(vencidos, Document.class, coleccion)) {
            eventos.add(mapear(documento));
        }
        return eventos;
    }

    @Override
    public boolean marcarEnviado(final String id, final Instant fechaEnvio) {
        final Query query = new Query(Criteria.where(ID).is(new ObjectId(id)).and(ESTADO)
                .ne(EstadoOutbox.ENVIADO.name()));
        final Update update = new Update().set(ESTADO, EstadoOutbox.ENVIADO.name()).set(ENVIO, fecha(fechaEnvio))
                .set(LEASE, null);
        return mongo.updateFirst(query, update, coleccion).getModifiedCount() > 0;
    }

    @Override
    public boolean marcarReintento(final AuditoriaOutboxEvento evento, final int intentos,
            final Instant proximoIntento, final String error) {
        final Update update = new Update().set(ESTADO, EstadoOutbox.PENDIENTE.name()).set(INTENTOS, intentos)
                .set(PROXIMO, fecha(proximoIntento)).set(LEASE, null).set(ERROR, error);
        return mongo.updateFirst(conLease(evento), update, coleccion).getModifiedCount() > 0;
    }

    @Override
    public boolean marcarFallido(final AuditoriaOutboxEvento evento, final int intentos, final String error) {
        final Update update = new Update().set(ESTADO, EstadoOutbox.FALLIDO.name()).set(INTENTOS, intentos)
                .set(LEASE, null).set(ERROR, error);
        return mongo.updateFirst(conLease(evento), update, coleccion).getModifiedCount() > 0;
    }

    @Override
    public boolean liberar(final AuditoriaOutboxEvento evento) {
        final Update update = new Update().set(ESTADO, EstadoOutbox.PENDIENTE.name()).set(LEASE, null);
        return mongo.updateFirst(conLease(evento), update, coleccion).getModifiedCount() > 0;
    }

    @Override
    public ResumenOutbox resumen() {
        final Criteria pendientes = Criteria.where(ESTADO).in(EstadoOutbox.PENDIENTE.name(),
                EstadoOutbox.EN_PROCESO.name());
        final long cantidad = mongo.count(new Query(pendientes), coleccion);
        final long fallidos = mongo.count(new Query(Criteria.where(ESTADO).is(EstadoOutbox.FALLIDO.name())), coleccion);
        final Query masAntiguo = new Query(pendientes).with(ORDEN).limit(1);
        masAntiguo.fields().include(CREACION);
        final Document primero = mongo.findOne(masAntiguo, Document.class, coleccion);
        Instant creacion = null;
        if (primero != null) {
            creacion = instante(primero, CREACION);
        }
        return new ResumenOutbox(cantidad, fallidos, creacion);
    }

    @Override
    public int limpiarEnviadosAnteriores(final Instant limite, final int maximo) {
        final Query viejos = new Query(Criteria.where(ESTADO).is(EstadoOutbox.ENVIADO.name())
                .and(ENVIO).lt(fecha(limite))).with(ORDEN).limit(maximo);
        viejos.fields().include(ID);
        final List<ObjectId> ids = new ArrayList<>();
        for (final Document documento : mongo.find(viejos, Document.class, coleccion)) {
            ids.add(documento.getObjectId(ID));
        }
        if (ids.isEmpty()) {
            return 0;
        }
        return (int) mongo.remove(new Query(Criteria.where(ID).in(ids).and(ESTADO).is(EstadoOutbox.ENVIADO.name())),
                coleccion).getDeletedCount();
    }

    private static Query conLease(final AuditoriaOutboxEvento evento) {
        return new Query(Criteria.where(ID).is(new ObjectId(evento.id()))
                .and(ESTADO).is(EstadoOutbox.EN_PROCESO.name())
                .and(LEASE).is(fecha(evento.leaseHasta())));
    }

    /** Mongo guarda milisegundos: el lease se compara por igualdad, así que todo se trunca igual. */
    private static Date fecha(final Instant instante) {
        return Date.from(instante.truncatedTo(ChronoUnit.MILLIS));
    }

    private static Instant instante(final Document documento, final String campo) {
        final Date valor = documento.getDate(campo);
        if (valor == null) {
            return null;
        }
        return valor.toInstant();
    }

    private static AuditoriaOutboxEvento mapear(final Document documento) {
        return new AuditoriaOutboxEvento(documento.getObjectId(ID).toHexString(), documento.getString(EVENT_ID),
                documento.getString(CLAVE), documento.getString(TOPIC), documento.getString(PAYLOAD),
                EstadoOutbox.valueOf(documento.getString(ESTADO)), documento.getInteger(INTENTOS, 0),
                instante(documento, PROXIMO), instante(documento, LEASE), instante(documento, CREACION),
                instante(documento, ENVIO), documento.getString(ERROR));
    }
}
