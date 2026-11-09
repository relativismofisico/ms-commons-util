package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEstadisticas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxMetricas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxProperties
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.OutboxAuditoriaRelay
import com.factoring.commons.auditoria.outbox.OutboxIntegracionBase
import com.factoring.commons.auditoria.outbox.RelojDePrueba
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant

/**
 * 0.6.1 contra un almacén real (MySQL o Mongo) y Kafka real: {@code eventId} propio, {@code eventId} repetido y tipos
 * compatibles con {@code JsonSerializer}. Las subclases dan el almacén.
 */
abstract class EventIdPropioBaseSpec extends Specification {

    /** Compatible, con un alias en {@code __TypeId__}. */
    static final String TIPO_ALIAS = "documento.emitido"
    /** Compatible, con el nombre completo de la clase en {@code __TypeId__}. */
    static final String TIPO_CLASE = "documento.archivado"
    /** Sin la opción: sale como sobre. */
    static final String TIPO_SOBRE = "documento.anotado"
    static final String ALIAS = "documentoEmitido"

    /** Payload con decimales, un instante con nanosegundos y texto no ASCII: lo que podría cambiar de bytes. */
    static record DocumentoEmitido(String id, BigDecimal cantidad, Instant emitidoEn, String titulo) {
    }

    static DocumentoEmitido documento(String id, String titulo = "Árbol ñandú — 😀 \"comillas\"") {
        new DocumentoEmitido(id, new BigDecimal("1234.50"), Instant.parse("2026-01-02T03:04:05.123456789Z"), titulo)
    }

    RelojDePrueba reloj = new RelojDePrueba()
    AuditoriaOutboxMetricas metricasOutbox
    AuditoriaOutboxProperties propiedades = new AuditoriaOutboxProperties(
            almacen: AuditoriaOutboxProperties.Almacen.JDBC, tamanoLote: 50, maxIntentos: 5,
            backoffBase: Duration.ofSeconds(1), backoffMax: Duration.ofSeconds(10), lease: Duration.ofMinutes(1),
            timeoutAck: Duration.ofSeconds(5))

    abstract AuditoriaOutboxPort almacen()

    abstract void enTransaccion(boolean revertir, Closure trabajo)

    abstract void escribirNegocio(String id)

    abstract boolean existeNegocio(String id)

    /** El {@code eventId} de cada fila o documento del outbox. */
    abstract List<String> eventIdsGuardados()

    /** El sobre guardado en la fila o documento con ese {@code eventId}. */
    abstract String sobreGuardado(String eventId)

    abstract void vaciar()

    def setup() {
        vaciar()
    }

    static TiposDeEvento tipos(String topicCompatible = "documentos.it", String topicSobre = "documentos.it") {
        def clave = { DocumentoEmitido d -> "DOC:" + d.id() }
        TiposDeEvento.de(
                TipoDeEvento.de(TIPO_ALIAS, DocumentoEmitido).topic(topicCompatible).clave(clave)
                        .compatibleConJsonSerializer(ALIAS).build(),
                TipoDeEvento.de(TIPO_CLASE, DocumentoEmitido).topic(topicCompatible).clave(clave)
                        .compatibleConJsonSerializer().build(),
                TipoDeEvento.de(TIPO_SOBRE, DocumentoEmitido).topic(topicSobre).clave(clave).build())
    }

    OutboxRegistro registro(TiposDeEvento declarados = tipos()) {
        new OutboxRegistro(almacen(), declarados, "ms-prueba", reloj, false)
    }

    OutboxAuditoriaRelay relay(PublicadorOutbox generico) {
        new OutboxAuditoriaRelay(almacen(), new OutboxIntegracionBase.Grabador(), generico, propiedades, reloj,
                OutboxAuditoriaRelay.Pausa.DORMIR)
    }

    /** El publicador de la autoconfiguración: template String/String propio, con los tipos del servicio. */
    static KafkaPublicadorOutbox publicadorKafka(TiposDeEvento declarados) {
        Map<String, Object> props = [
                (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)     : KafkaDePrueba.kafka().bootstrapServers,
                (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)  : StringSerializer,
                (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG): StringSerializer,
                (ProducerConfig.ACKS_CONFIG)                  : "all",
                (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)    : true,
                (ProducerConfig.LINGER_MS_CONFIG)             : 0]
        new KafkaPublicadorOutbox(new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(props)), declarados)
    }

    static <V> List<ConsumerRecord<String, V>> leer(String topic, int esperados, Class deserializador,
            Map<String, Object> extra = [:]) {
        Map<String, Object> props = [
                (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)       : KafkaDePrueba.kafka().bootstrapServers,
                (ConsumerConfig.GROUP_ID_CONFIG)                : "lector-" + UUID.randomUUID(),
                (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)       : "earliest",
                (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)  : StringDeserializer,
                (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): deserializador] + extra
        List<ConsumerRecord<String, V>> leidos = []
        new KafkaConsumer<String, V>(props).withCloseable { consumidor ->
            consumidor.subscribe([topic])
            long hasta = System.currentTimeMillis() + 30_000
            while (System.currentTimeMillis() < hasta && leidos.size() < esperados) {
                consumidor.poll(Duration.ofMillis(500)).each { leidos << it }
            }
        }
        leidos
    }

    static String cabecera(ConsumerRecord<String, ?> registro, String nombre) {
        def h = registro.headers().lastHeader(nombre)
        h == null ? null : new String(h.value(), StandardCharsets.UTF_8)
    }

    def "eventId propio: es el mismo en la fila, en el sobre guardado y en la cabecera del mensaje"() {
        given:
        def topic = "documentos.it." + UUID.randomUUID()
        def declarados = tipos(topic, topic)

        when:
        Optional<EventoOutbox<DocumentoEmitido>> evento = null
        enTransaccion(false) {
            escribirNegocio("N-1")
            evento = registro(declarados).registrar(TIPO_SOBRE, "ID-NEGOCIO-1", documento("1"))
        }
        def enviados = relay(publicadorKafka(declarados)).ejecutarCiclo()
        def leidos = leer(topic, 1, StringDeserializer)

        then:
        existeNegocio("N-1")
        evento.isPresent()
        evento.get().eventId() == "ID-NEGOCIO-1"
        eventIdsGuardados() == ["ID-NEGOCIO-1"]
        OutboxJson.deserializar(sobreGuardado("ID-NEGOCIO-1"), DocumentoEmitido).eventId() == "ID-NEGOCIO-1"
        enviados == 1
        leidos.size() == 1
        cabecera(leidos[0], EventoOutbox.CABECERA_EVENT_ID) == "ID-NEGOCIO-1"
        OutboxJson.deserializar(leidos[0].value(), DocumentoEmitido).eventId() == "ID-NEGOCIO-1"
        OutboxJson.deserializar(leidos[0].value(), DocumentoEmitido).payload() == documento("1")
    }

    def "eventId repetido, en la misma transacción y en otra: no se escribe otro evento, el negocio confirma y se cuenta"() {
        given:
        def r = registro()
        def metricas = new SimpleMeterRegistry()
        // El FunctionCounter de Micrometer guarda su objeto con una referencia débil: como el bean de la
        // autoconfiguración, el test retiene la instancia de las métricas (y con ella la función) en un campo.
        metricasOutbox = new AuditoriaOutboxMetricas(new AuditoriaOutboxEstadisticas(almacen(), reloj), "ms-prueba",
                tipos().nombres(), r::duplicados)
        metricasOutbox.bindTo(metricas)

        when: "dos veces en la misma transacción"
        Optional primero = null
        Optional segundo = null
        enTransaccion(false) {
            escribirNegocio("N-2")
            primero = r.registrar(TIPO_ALIAS, "ID-2", documento("2", "original"))
            segundo = r.registrar(TIPO_ALIAS, "ID-2", documento("2", "otro"))
        }

        then:
        existeNegocio("N-2")
        primero.isPresent()
        segundo.isEmpty()
        eventIdsGuardados() == ["ID-2"]

        when: "otra vez en otra transacción, ya confirmado el primero"
        Optional tercero = null
        enTransaccion(false) {
            escribirNegocio("N-3")
            tercero = r.registrar(TIPO_ALIAS, "ID-2", documento("2", "tercero"))
        }

        then: "el negocio de la segunda transacción también confirma; el evento guardado es el primero"
        existeNegocio("N-3")
        tercero.isEmpty()
        eventIdsGuardados() == ["ID-2"]
        OutboxJson.deserializar(sobreGuardado("ID-2"), DocumentoEmitido).payload().titulo() == "original"
        r.duplicados(TIPO_ALIAS) == 2
        r.duplicados(TIPO_CLASE) == 0
        metricas.get(AuditoriaOutboxMetricas.OUTBOX_DUPLICADOS).tag("tipo", TIPO_ALIAS).tag("servicio", "ms-prueba")
                .functionCounter().count() == 2d
    }

    def "un rollback no deja el eventId ocupado: el reintento del caso de uso lo registra"() {
        when:
        enTransaccion(true) {
            escribirNegocio("N-4")
            registro().registrar(TIPO_ALIAS, "ID-4", documento("4"))
        }
        Optional reintento = null
        enTransaccion(false) {
            escribirNegocio("N-4")
            reintento = registro().registrar(TIPO_ALIAS, "ID-4", documento("4"))
        }

        then:
        reintento.isPresent()
        existeNegocio("N-4")
        eventIdsGuardados() == ["ID-4"]
    }

    def "eventId inválido (#descripcion): se rechaza y no se guarda nada"() {
        when:
        registro().registrar(TIPO_ALIAS, eventId, documento("5"))

        then:
        thrown(IllegalArgumentException)
        eventIdsGuardados().isEmpty()

        where:
        descripcion  | eventId
        "nulo"       | null
        "vacío"      | ""
        "en blanco"  | "   "
        "65 chars"   | "x" * 65
    }

    def "64 caracteres es el máximo admitido"() {
        expect:
        registro().registrar(TIPO_ALIAS, "x" * 64, documento("6")).isPresent()
        eventIdsGuardados() == ["x" * 64]
    }

    def "tipo compatible: valor idéntico byte a byte al JsonSerializer, __TypeId__ y cabeceras del sobre; el no compatible sale como sobre"() {
        given:
        def topicCompatible = "documentos.compatibles." + UUID.randomUUID()
        def topicSobre = "documentos.sobre." + UUID.randomUUID()
        def declarados = tipos(topicCompatible, topicSobre)
        def r = registro(declarados)
        r.registrar(TIPO_ALIAS, "ID-A", documento("A"))
        r.registrar(TIPO_CLASE, "ID-C", documento("C"))
        r.registrar(TIPO_SOBRE, "ID-S", documento("S"))

        when:
        def enviados = relay(publicadorKafka(declarados)).ejecutarCiclo()
        List<ConsumerRecord<String, byte[]>> compatibles = leer(topicCompatible, 2, ByteArrayDeserializer)
        List<ConsumerRecord<String, String>> sobres = leer(topicSobre, 1, StringDeserializer)
        def porId = compatibles.collectEntries { [(cabecera(it, EventoOutbox.CABECERA_EVENT_ID)): it] }

        then:
        enviados == 3
        compatibles.size() == 2
        with(porId["ID-A"] as ConsumerRecord<String, byte[]>) {
            key() == "DOC:A"
            value() == new JsonSerializer<Object>().serialize(topicCompatible, documento("A"))
            cabecera(it, TipoDeEvento.CABECERA_TIPO_SPRING) == ALIAS
            cabecera(it, EventoOutbox.CABECERA_TIPO) == TIPO_ALIAS
            cabecera(it, EventoOutbox.CABECERA_VERSION) == "1"
            cabecera(it, EventoOutbox.CABECERA_SERVICIO_ORIGEN) == "ms-prueba"
        }
        with(porId["ID-C"] as ConsumerRecord<String, byte[]>) {
            key() == "DOC:C"
            value() == new JsonSerializer<Object>().serialize(topicCompatible, documento("C"))
            cabecera(it, TipoDeEvento.CABECERA_TIPO_SPRING) == DocumentoEmitido.name
        }
        sobres.size() == 1
        cabecera(sobres[0], TipoDeEvento.CABECERA_TIPO_SPRING) == null
        cabecera(sobres[0], EventoOutbox.CABECERA_EVENT_ID) == "ID-S"
        OutboxJson.deserializar(sobres[0].value(), DocumentoEmitido).payload() == documento("S")
    }

    def "tipo compatible: un consumidor real con JsonDeserializer y __TypeId__ lo lee con su configuración de siempre"() {
        given:
        def topic = "documentos.consumidor." + UUID.randomUUID()
        def declarados = tipos(topic, "documentos.sobre." + UUID.randomUUID())
        def r = registro(declarados)
        r.registrar(TIPO_ALIAS, "ID-1", documento("1"))
        r.registrar(TIPO_CLASE, "ID-2", documento("2"))
        relay(publicadorKafka(declarados)).ejecutarCiclo()

        when: "la configuración típica de un consumidor existente: tipo por cabecera, mapeo de alias y paquetes de confianza"
        List<ConsumerRecord<String, Object>> leidos = leer(topic, 2, JsonDeserializer, [
                (JsonDeserializer.TRUSTED_PACKAGES): DocumentoEmitido.package.name,
                (JsonDeserializer.TYPE_MAPPINGS)   : ALIAS + ":" + DocumentoEmitido.name])

        then:
        leidos*.value() as Set == [documento("1"), documento("2")] as Set
        leidos.every { it.value() instanceof DocumentoEmitido }
    }
}
