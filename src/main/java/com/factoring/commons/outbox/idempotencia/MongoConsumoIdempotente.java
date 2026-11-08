package com.factoring.commons.outbox.idempotencia;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * {@link ConsumoIdempotente} sobre una colección del MongoDB del servicio, con un {@code MongoTransactionManager}
 * (requiere replica set). El {@code _id} de la marca es {@code {consumidor, clave}}: la unicidad no depende de un
 * índice. La retención la aplica un índice TTL sobre {@code fechaProcesado} (lo crea {@link #crearIndices()}).
 *
 * <p>En Mongo un error de clave duplicada aborta la transacción: el duplicado se resuelve marcándola para revertir
 * (no hay nada que confirmar). Por eso conviene llamarlo fuera de otra transacción de Mongo: si se une a una en curso,
 * esa también queda marcada para revertir. Dos entregas concurrentes del mismo evento: una aplica el efecto y la otra
 * recibe un conflicto de escritura transitorio, que se propaga para que el consumidor la reintente (el reintento ya
 * encuentra la marca).</p>
 */
public class MongoConsumoIdempotente implements ConsumoIdempotente {

    /** Colección por defecto. */
    public static final String COLECCION = "evento_procesado";

    private static final String ID = "_id";
    private static final String FECHA = "fechaProcesado";
    private static final String TTL = "ttl_evento_procesado";

    private final MongoOperations mongo;
    private final TransactionTemplate transaccion;
    private final String coleccion;
    private final Duration retencion;
    private final Clock reloj;

    /**
     * @param mongo         el {@code MongoTemplate} del servicio
     * @param transacciones un {@code MongoTransactionManager} sobre la misma base
     * @param coleccion     colección de las marcas
     * @param retencion     cuánto se conserva cada marca (índice TTL)
     * @param reloj         reloj de {@code fechaProcesado}
     */
    public MongoConsumoIdempotente(final MongoOperations mongo, final PlatformTransactionManager transacciones,
            final String coleccion, final Duration retencion, final Clock reloj) {
        this.mongo = mongo;
        this.transaccion = new TransactionTemplate(transacciones);
        this.coleccion = coleccion;
        this.retencion = retencion;
        this.reloj = reloj;
    }

    /** Crea la colección (si no existe) y el índice TTL de la retención; si ya existía con otra, la ajusta. */
    public void crearIndices() {
        if (!mongo.collectionExists(coleccion)) {
            mongo.createCollection(coleccion);
        }
        try {
            mongo.indexOps(coleccion).createIndex(new Index().on(FECHA, Sort.Direction.ASC).expire(retencion)
                    .named(TTL));
        } catch (DataAccessException conflicto) {
            mongo.executeCommand(new Document("collMod", coleccion).append("index", new Document("name", TTL)
                    .append("expireAfterSeconds", retencion.toSeconds())));
        }
    }

    @Override
    public boolean procesar(final String consumidor, final String eventId, final Runnable efecto) {
        ConsumoIdempotente.validar(consumidor, eventId);
        final Boolean aplicado = transaccion.execute(estado -> {
            try {
                mongo.insert(new Document(ID, new Document("consumidor", consumidor).append("clave", eventId))
                        .append(FECHA, Date.from(reloj.instant())), coleccion);
            } catch (DuplicateKeyException duplicado) {
                estado.setRollbackOnly();
                return Boolean.FALSE;
            }
            efecto.run();
            return Boolean.TRUE;
        });
        return Boolean.TRUE.equals(aplicado);
    }

    @Override
    public int limpiarAnteriores(final Instant limite, final int maximo) {
        final Query viejas = new Query(Criteria.where(FECHA).lt(Date.from(limite))).limit(maximo);
        viejas.fields().include(ID);
        final List<Object> ids = new ArrayList<>();
        for (final Document marca : mongo.find(viejas, Document.class, coleccion)) {
            ids.add(marca.get(ID));
        }
        if (ids.isEmpty()) {
            return 0;
        }
        return (int) mongo.remove(new Query(Criteria.where(ID).in(ids)), coleccion).getDeletedCount();
    }
}
