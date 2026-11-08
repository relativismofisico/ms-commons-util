package com.factoring.commons.auditoria.outbox.mongo;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento;
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort;
import com.factoring.commons.auditoria.outbox.EstadoOutbox;
import com.factoring.commons.auditoria.outbox.OrdenPorClave;
import com.factoring.commons.auditoria.outbox.ResumenOutbox;
import com.factoring.commons.auditoria.outbox.ResumenTipo;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * {@link AuditoriaOutboxPort} sobre una colección propia del servicio en su MongoDB.
 *
 * <p>{@link #guardar} usa el {@code MongoTemplate} del servicio, así participa en la transacción activa de un
 * {@code MongoTransactionManager} (requiere replica set). Sin transacción de Mongo el insert se confirma solo, en el
 * momento. El reclamo es un {@code updateMany} condicionado a {@code PENDIENTE} y marcado con un token de la llamada:
 * dos relays nunca reclaman el mismo evento.</p>
 *
 * <p>Índices (los crea {@link #crearIndices()} al arrancar; ver el README): {@code eventId} único;
 * {@code {estado, fechaCreacion, _id}} para recorrer los candidatos en orden; {@code {estado, proximoIntento,
 * fechaCreacion, _id}} para los bloqueos por backoff; {@code {claveKafka, estado, fechaCreacion}} para el orden por
 * clave; {@code {estado, fechaEnvio}} para la limpieza; y, con retención, un TTL parcial sobre {@code fechaEnvio} de
 * los ENVIADO.</p>
 */
public class MongoAuditoriaOutboxAdapter implements AuditoriaOutboxPort {

    /** Candidatos leídos por página al armar un lote. */
    static final int PAGINA = 1000;

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
    private static final String TIPO = "tipo";
    private static final String RECLAMO = "reclamo";
    private static final String TTL_ENVIADOS = "ttl_outbox_enviados";
    private static final Sort ORDEN = Sort.by(Sort.Direction.ASC, CREACION).and(Sort.by(Sort.Direction.ASC, ID));

    private final MongoOperations mongo;
    private final String coleccion;
    private final Duration retencionEnviados;

    /**
     * Adaptador sin índice TTL (la limpieza periódica borra los ENVIADO).
     *
     * @param mongo     el {@code MongoTemplate} del servicio
     * @param coleccion colección del outbox, exclusiva del servicio
     */
    public MongoAuditoriaOutboxAdapter(final MongoOperations mongo, final String coleccion) {
        this(mongo, coleccion, null);
    }

    /**
     * @param mongo             el {@code MongoTemplate} del servicio
     * @param coleccion         colección del outbox, exclusiva del servicio
     * @param retencionEnviados cuánto se conservan los ENVIADO antes de que los borre el índice TTL ({@code null}: sin
     *                          TTL)
     */
    public MongoAuditoriaOutboxAdapter(final MongoOperations mongo, final String coleccion,
            final Duration retencionEnviados) {
        this.mongo = mongo;
        this.coleccion = coleccion;
        this.retencionEnviados = retencionEnviados;
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
        indices.createIndex(new CompoundIndexDefinition(new Document(ESTADO, 1).append(CREACION, 1).append(ID, 1))
                .named("ix_outbox_orden"));
        indices.createIndex(new CompoundIndexDefinition(new Document(CLAVE, 1).append(ESTADO, 1).append(CREACION, 1))
                .named("ix_auditoria_outbox_clave"));
        indices.createIndex(new CompoundIndexDefinition(new Document(ESTADO, 1).append(ENVIO, 1))
                .named("ix_auditoria_outbox_limpieza"));
        if (retencionEnviados != null) {
            crearTtl(indices);
        }
    }

    /** TTL parcial sobre los ENVIADO; si ya existía con otra retención, la ajusta con {@code collMod}. */
    private void crearTtl(final IndexOperations indices) {
        final Index ttl = new Index().on(ENVIO, Sort.Direction.ASC).expire(retencionEnviados)
                .partial(PartialIndexFilter.of(Criteria.where(ESTADO).is(EstadoOutbox.ENVIADO.name())))
                .named(TTL_ENVIADOS);
        try {
            indices.createIndex(ttl);
        } catch (DataAccessException conflicto) {
            mongo.executeCommand(new Document("collMod", coleccion).append("index", new Document("name", TTL_ENVIADOS)
                    .append("expireAfterSeconds", retencionEnviados.toSeconds())));
        }
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
        if (evento.tipo() != null) {
            documento.append(TIPO, evento.tipo());
        }
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
        final List<ObjectId> aReclamar = new ArrayList<>();
        for (final Document candidato : candidatos) {
            if (elegibles.contains(candidato.getObjectId(ID).toHexString())) {
                aReclamar.add(candidato.getObjectId(ID));
            }
        }
        if (aReclamar.isEmpty()) {
            return List.of();
        }
        return reclamar(candidatos, aReclamar, fecha(leaseHasta));
    }

    /**
     * Reclama los elegibles con un solo {@code updateMany} condicionado a {@code PENDIENTE} y marcado con un token
     * propio de esta llamada (una sola espera de escritura, en vez de una por evento), y lee lo que quedó con ese
     * token. Si otro relay se llevó antes alguno, los siguientes de su clave que reclamó esta llamada se liberan: el
     * orden por clave se conserva igual que reclamando de a uno.
     */
    private List<AuditoriaOutboxEvento> reclamar(final List<Document> candidatos, final List<ObjectId> aReclamar,
            final Date lease) {
        final String token = UUID.randomUUID().toString();
        mongo.updateMulti(new Query(Criteria.where(ID).in(aReclamar).and(ESTADO).is(EstadoOutbox.PENDIENTE.name())),
                new Update().set(ESTADO, EstadoOutbox.EN_PROCESO.name()).set(LEASE, lease).set(RECLAMO, token),
                coleccion);
        final Map<ObjectId, Document> propios = new HashMap<>();
        for (final Document reclamado : mongo.find(new Query(Criteria.where(ID).in(aReclamar).and(RECLAMO).is(token)
                .and(ESTADO).is(EstadoOutbox.EN_PROCESO.name())), Document.class, coleccion)) {
            propios.put(reclamado.getObjectId(ID), reclamado);
        }
        final Set<ObjectId> elegidos = new HashSet<>(aReclamar);
        final Set<String> clavesDetenidas = new HashSet<>();
        final List<ObjectId> aLiberar = new ArrayList<>();
        final List<AuditoriaOutboxEvento> reclamados = new ArrayList<>();
        for (final Document candidato : candidatos) {
            final ObjectId id = candidato.getObjectId(ID);
            if (!elegidos.contains(id)) {
                continue;
            }
            final String clave = candidato.getString(CLAVE);
            final Document propio = propios.get(id);
            if (propio == null) {
                // Otro relay lo tomó: los siguientes de esta clave tampoco son nuestros.
                clavesDetenidas.add(clave);
            } else if (clavesDetenidas.contains(clave)) {
                aLiberar.add(id);
            } else {
                reclamados.add(mapear(propio));
            }
        }
        if (!aLiberar.isEmpty()) {
            mongo.updateMulti(new Query(Criteria.where(ID).in(aLiberar).and(RECLAMO).is(token)
                            .and(ESTADO).is(EstadoOutbox.EN_PROCESO.name())),
                    new Update().set(ESTADO, EstadoOutbox.PENDIENTE.name()).set(LEASE, null), coleccion);
        }
        return reclamados;
    }

    /**
     * Pendientes listos sin un anterior de su clave que NO pueda ir en este lote (FALLIDO, EN_PROCESO o PENDIENTE en
     * backoff), en orden de creación. Se recorren por páginas sobre {@code {estado, fechaCreacion, _id}} (el orden sale
     * del índice, sin ordenar en memoria) y con proyección sin {@code payload}. Para cada página se calcula, con un
     * solo {@code $group}, el primer bloqueante de cada una de sus claves; un candidato posterior a ese bloqueante se
     * salta. Se para al llenar el lote: una clave bloqueada nunca ocupa el lote y las demás claves siempre salen.
     */
    private List<Document> candidatos(final int tamano, final Date ahora) {
        final List<Document> elegidos = new ArrayList<>();
        Document ultimo = null;
        while (elegidos.size() < tamano) {
            final List<Document> pagina = paginaDeCandidatos(ahora, ultimo);
            if (pagina.isEmpty()) {
                break;
            }
            final Map<String, Document> bloqueos = primerBloqueoPorClave(pagina, ahora);
            for (final Document candidato : pagina) {
                if (!bloqueado(candidato, bloqueos.get(candidato.getString(CLAVE)))) {
                    elegidos.add(candidato);
                    if (elegidos.size() == tamano) {
                        break;
                    }
                }
            }
            if (pagina.size() < PAGINA) {
                break;
            }
            ultimo = pagina.get(pagina.size() - 1);
        }
        return elegidos;
    }

    private List<Document> paginaDeCandidatos(final Date ahora, final Document despuesDe) {
        Criteria criterio = Criteria.where(ESTADO).is(EstadoOutbox.PENDIENTE.name()).and(PROXIMO).lte(ahora);
        if (despuesDe != null) {
            final Date creacion = despuesDe.getDate(CREACION);
            criterio = new Criteria().andOperator(criterio, new Criteria().orOperator(
                    Criteria.where(CREACION).gt(creacion),
                    Criteria.where(CREACION).is(creacion).and(ID).gt(despuesDe.getObjectId(ID))));
        }
        final Query pagina = new Query(criterio).with(ORDEN).limit(PAGINA);
        pagina.fields().include(ID, CLAVE, CREACION);
        return mongo.find(pagina, Document.class, coleccion);
    }

    /** El primer evento bloqueante (FALLIDO, EN_PROCESO o PENDIENTE en backoff) de cada clave de la página. */
    private Map<String, Document> primerBloqueoPorClave(final List<Document> pagina, final Date ahora) {
        final Set<String> claves = new LinkedHashSet<>();
        for (final Document candidato : pagina) {
            claves.add(candidato.getString(CLAVE));
        }
        final Date hasta = pagina.get(pagina.size() - 1).getDate(CREACION);
        final Document filtro = new Document(CLAVE, new Document("$in", new ArrayList<>(claves)))
                .append(CREACION, new Document("$lte", hasta))
                .append("$or", List.of(
                        new Document(ESTADO, new Document("$in",
                                List.of(EstadoOutbox.FALLIDO.name(), EstadoOutbox.EN_PROCESO.name()))),
                        new Document(ESTADO, EstadoOutbox.PENDIENTE.name()).append(PROXIMO, new Document("$gt", ahora))));
        final List<Document> etapas = List.of(
                new Document("$match", filtro),
                new Document("$sort", new Document(CREACION, 1).append(ID, 1)),
                new Document("$group", new Document(ID, "$" + CLAVE)
                        .append(CREACION, new Document("$first", "$" + CREACION))
                        .append("primerId", new Document("$first", "$" + ID))));
        final Map<String, Document> bloqueos = new HashMap<>();
        for (final Document bloqueo : agregar(etapas)) {
            bloqueos.put(bloqueo.getString(ID), bloqueo);
        }
        return bloqueos;
    }

    /** Si el bloqueante de su clave es anterior al candidato (por fecha de creación y, a igual fecha, por id). */
    private static boolean bloqueado(final Document candidato, final Document bloqueo) {
        if (bloqueo == null) {
            return false;
        }
        final int porFecha = bloqueo.getDate(CREACION).compareTo(candidato.getDate(CREACION));
        return porFecha < 0
                || porFecha == 0 && bloqueo.getObjectId("primerId").compareTo(candidato.getObjectId(ID)) < 0;
    }

    private List<Document> agregar(final List<Document> etapas) {
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
        ResumenTipo total = ResumenTipo.VACIO;
        for (final ResumenTipo parcial : resumenPorTipo().values()) {
            total = sumar(total, parcial);
        }
        return new ResumenOutbox(total.pendientes(), total.fallidos(), total.masAntiguo());
    }

    @Override
    public Map<String, ResumenTipo> resumenPorTipo() {
        final String pendiente = EstadoOutbox.PENDIENTE.name();
        final String enProceso = EstadoOutbox.EN_PROCESO.name();
        final Document esPendiente = new Document("$in", List.of("$" + ESTADO, List.of(pendiente, enProceso)));
        final List<Document> etapas = List.of(
                new Document("$match", new Document(ESTADO, new Document("$ne", EstadoOutbox.ENVIADO.name()))),
                new Document("$group", new Document(ID, new Document("$ifNull",
                        List.of("$" + TIPO, AuditoriaOutboxEvento.TIPO_AUDITORIA)))
                        .append("pendientes", contarSi(esPendiente))
                        .append("fallidos", contarSi(new Document("$eq", List.of("$" + ESTADO,
                                EstadoOutbox.FALLIDO.name()))))
                        .append("enProceso", contarSi(new Document("$eq", List.of("$" + ESTADO, enProceso))))
                        .append("masAntiguo", new Document("$min", new Document("$cond",
                                Arrays.asList(esPendiente, "$" + CREACION, null))))));
        final Map<String, ResumenTipo> porTipo = new LinkedHashMap<>();
        for (final Document grupo : agregar(etapas)) {
            final Date masAntiguo = grupo.getDate("masAntiguo");
            Instant creacion = null;
            if (masAntiguo != null) {
                creacion = masAntiguo.toInstant();
            }
            porTipo.put(grupo.getString(ID), new ResumenTipo(numero(grupo, "pendientes"), numero(grupo, "enProceso"),
                    numero(grupo, "fallidos"), creacion));
        }
        return porTipo;
    }

    private static Document contarSi(final Document condicion) {
        return new Document("$sum", new Document("$cond", List.of(condicion, 1, 0)));
    }

    private static long numero(final Document documento, final String campo) {
        final Number valor = documento.get(campo, Number.class);
        if (valor == null) {
            return 0;
        }
        return valor.longValue();
    }

    private static ResumenTipo sumar(final ResumenTipo a, final ResumenTipo b) {
        Instant masAntiguo = a.masAntiguo();
        if (masAntiguo == null || b.masAntiguo() != null && b.masAntiguo().isBefore(masAntiguo)) {
            masAntiguo = b.masAntiguo();
        }
        return new ResumenTipo(a.pendientes() + b.pendientes(), a.enProceso() + b.enProceso(),
                a.fallidos() + b.fallidos(), masAntiguo);
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
                instante(documento, ENVIO), documento.getString(ERROR), documento.getString(TIPO));
    }
}
