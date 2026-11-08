package com.factoring.commons.outbox

import com.factoring.commons.auditoria.AuditoriaAccionEvent
import com.factoring.commons.auditoria.AuditoriaPublisher
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEstadisticas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxLimpieza
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxMetricas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPlanificador
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxProperties
import com.factoring.commons.auditoria.outbox.BloqueoLimpieza
import com.factoring.commons.auditoria.outbox.EstadoOutbox
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.OutboxAuditoriaPort
import com.factoring.commons.auditoria.outbox.OutboxAuditoriaRelay
import com.factoring.commons.auditoria.outbox.OutboxIntegracionBase
import com.factoring.commons.auditoria.outbox.RelojDePrueba
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Outbox genérico contra un almacén real (MySQL o Mongo): las subclases dan el almacén y sus utilidades.
 */
abstract class OutboxGenericoBaseSpec extends Specification {

    static final String TIPO = "documento.creado"
    static final String TOPIC = "documentos.it"

    RelojDePrueba reloj = new RelojDePrueba()
    AuditoriaOutboxProperties propiedades = new AuditoriaOutboxProperties(
            almacen: AuditoriaOutboxProperties.Almacen.JDBC, tamanoLote: 50, maxIntentos: 5,
            backoffBase: Duration.ofSeconds(1), backoffMax: Duration.ofSeconds(10), lease: Duration.ofMinutes(1),
            timeoutAck: Duration.ofSeconds(5))

    abstract AuditoriaOutboxPort almacen()

    abstract void enTransaccion(boolean revertir, Closure trabajo)

    abstract void escribirNegocio(String id)

    abstract boolean existeNegocio(String id)

    abstract AuditoriaOutboxEvento leer(String eventId)

    abstract void forzar(String eventId, EstadoOutbox estado, Instant proximoIntento)

    abstract void vaciar()

    def setup() {
        vaciar()
    }

    TiposDeEvento tipos(String topic = TOPIC) {
        TiposDeEvento.de(TipoDeEvento.de(TIPO, Map).topic(topic).clave({ Map m -> "DOC:" + m.id }).build())
    }

    OutboxRegistro registro(String topic = TOPIC, boolean soloDrenar = false) {
        new OutboxRegistro(almacen(), tipos(topic), "ms-prueba", reloj, soloDrenar)
    }

    OutboxAuditoriaRelay relay(PublicadorOutbox generico, AuditoriaPublisher auditoria = new OutboxIntegracionBase.Grabador()) {
        new OutboxAuditoriaRelay(almacen(), auditoria, generico, propiedades, reloj, OutboxAuditoriaRelay.Pausa.DORMIR)
    }

    /** Publicador que registra lo que "Kafka confirmó", con sus cabeceras. */
    static class GrabadorGenerico implements PublicadorOutbox {
        final List<Map> publicados = new CopyOnWriteArrayList<>()

        void publicarConfirmado(String topic, String clave, String valor, Map<String, String> cabeceras, Duration espera) {
            publicados << [topic: topic, clave: clave, valor: valor, cabeceras: cabeceras]
        }
    }

    static KafkaPublicadorOutbox publicadorKafka() {
        Map<String, Object> props = [
                (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)     : KafkaDePrueba.kafka().bootstrapServers,
                (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)  : StringSerializer,
                (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG): StringSerializer,
                (ProducerConfig.ACKS_CONFIG)                  : "all",
                (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)    : true,
                (ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)    : 2000,
                (ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)   : 4000,
                (ProducerConfig.LINGER_MS_CONFIG)             : 0,
                (ProducerConfig.MAX_BLOCK_MS_CONFIG)          : 3000]
        new KafkaPublicadorOutbox(new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(props)))
    }

    static List<ConsumerRecord<String, String>> leerTopic(String topic, int esperados) {
        Map<String, Object> props = [
                (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)       : KafkaDePrueba.kafka().bootstrapServers,
                (ConsumerConfig.GROUP_ID_CONFIG)                : "lector-" + UUID.randomUUID(),
                (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)       : "earliest",
                (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)  : StringDeserializer,
                (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer]
        List<ConsumerRecord<String, String>> leidos = []
        new KafkaConsumer<String, String>(props).withCloseable { consumidor ->
            consumidor.subscribe([topic])
            long hasta = System.currentTimeMillis() + 30_000
            while (System.currentTimeMillis() < hasta && leidos.size() < esperados) {
                consumidor.poll(Duration.ofMillis(500)).each { leidos << it }
            }
        }
        leidos
    }

    static String cabecera(ConsumerRecord<String, String> registro, String nombre) {
        new String(registro.headers().lastHeader(nombre).value(), StandardCharsets.UTF_8)
    }

    def "registrar dentro de una transacción confirmada: queda PENDIENTE con su tipo, junto con el cambio de negocio"() {
        when:
        EventoOutbox<Map> evento = null
        enTransaccion(false) {
            escribirNegocio("N-1")
            evento = registro().registrar(TIPO, [id: "1", titulo: "uno"])
        }

        then:
        existeNegocio("N-1")
        with(leer(evento.eventId())) {
            estado() == EstadoOutbox.PENDIENTE
            tipo() == TIPO
            claveKafka() == "DOC:1"
            topic() == TOPIC
            OutboxJson.deserializar(payload(), Map) == evento
        }
    }

    def "rollback de la transacción: no queda ni el cambio de negocio ni el evento"() {
        when:
        EventoOutbox<Map> evento = null
        enTransaccion(true) {
            escribirNegocio("N-2")
            evento = registro().registrar(TIPO, [id: "2"])
        }

        then:
        !existeNegocio("N-2")
        leer(evento.eventId()) == null
    }

    def "el relay publica en Kafka el sobre tal cual, con su clave y las cabeceras eventId, tipo, version y servicioOrigen"() {
        given:
        def topic = "documentos.it." + UUID.randomUUID()
        def evento = registro(topic).registrar(TIPO, [id: "3", titulo: "tres"])

        when:
        def enviados = relay(publicadorKafka()).ejecutarCiclo()
        def leidos = leerTopic(topic, 1)

        then:
        enviados == 1
        leer(evento.eventId()).estado() == EstadoOutbox.ENVIADO
        leidos.size() == 1
        leidos[0].key() == "DOC:3"
        cabecera(leidos[0], EventoOutbox.CABECERA_EVENT_ID) == evento.eventId()
        cabecera(leidos[0], EventoOutbox.CABECERA_TIPO) == TIPO
        cabecera(leidos[0], EventoOutbox.CABECERA_VERSION) == "1"
        cabecera(leidos[0], EventoOutbox.CABECERA_SERVICIO_ORIGEN) == "ms-prueba"
        OutboxJson.deserializar(leidos[0].value(), Map) == evento
        // Ni headers de tipo de Spring: el consumidor no debe depender de clases del productor
        leidos[0].headers().lastHeader("__TypeId__") == null
    }

    def "auditoría y eventos genéricos en el mismo almacén: cada uno sale por su publicador, y el resumen se separa por tipo"() {
        given:
        def auditoria = new OutboxAuditoriaPort(almacen(), "auditoria.accion", "ms-prueba", reloj)
        def accion = OutboxIntegracionBase.evento("A-1")
        auditoria.registrar(accion)
        reloj.avanzar(Duration.ofSeconds(5))
        def d1 = registro().registrar(TIPO, [id: "4"])
        registro().registrar(TIPO, [id: "5"])

        expect:
        with(almacen().resumenPorTipo()) {
            get(AuditoriaOutboxEvento.TIPO_AUDITORIA).pendientes() == 1
            get(TIPO).pendientes() == 2
            get(TIPO).masAntiguo() == d1.ocurridoEn()
        }
        almacen().resumen().pendientes() == 3

        when:
        def grabadorAuditoria = new OutboxIntegracionBase.Grabador()
        def grabadorGenerico = new GrabadorGenerico()
        def enviados = relay(grabadorGenerico, grabadorAuditoria).ejecutarCiclo()

        then:
        enviados == 3
        grabadorAuditoria.ids() == [accion.eventId()]
        grabadorGenerico.publicados*.clave == ["DOC:4", "DOC:5"]
        grabadorGenerico.publicados.every { it.topic == TOPIC && it.cabeceras.tipo == TIPO }
        almacen().resumen().pendientes() == 0
    }

    def "muchas claves bloqueadas al principio (FALLIDO y backoff): el lote se llena con las demás claves y nunca toma de las bloqueadas"() {
        given: "30 claves de 25 eventos cada una, creadas primero: 15 con un FALLIDO en cabeza y 15 con la cabeza en backoff"
        def r = registro()
        def cabezas = []
        (1..30).each { clave ->
            (1..25).each { n ->
                def e = r.registrar(TIPO, [id: "B" + clave])
                if (n == 1) {
                    cabezas << e.eventId()
                }
            }
        }
        cabezas.eachWithIndex { id, i ->
            if (i < 15) {
                forzar(id, EstadoOutbox.FALLIDO, reloj.instant())
            } else {
                forzar(id, EstadoOutbox.PENDIENTE, reloj.instant().plus(Duration.ofHours(1)))
            }
        }
        and: "detrás, 60 eventos de 60 claves libres"
        reloj.avanzar(Duration.ofMillis(5))
        def libres = (1..60).collect { r.registrar(TIPO, [id: "L" + it]).eventId() }

        when:
        def primero = almacen().tomarLote(50, reloj.instant(), reloj.instant().plus(propiedades.lease))
        def segundo = almacen().tomarLote(50, reloj.instant(), reloj.instant().plus(propiedades.lease))

        then:
        primero.size() == 50
        segundo.size() == 10
        (primero + segundo)*.eventId() as Set == libres as Set
        (primero + segundo).every { it.claveKafka().startsWith("DOC:L") }
    }

    def "solo drenar: el puerto genérico rechaza eventos nuevos, la auditoría va directo y el relay drena lo pendiente"() {
        given:
        def pendiente = registro().registrar(TIPO, [id: "6"])
        def directo = Mock(AuditoriaPublisher)
        def auditoria = new OutboxAuditoriaPort(almacen(), "auditoria.accion", "ms-prueba", reloj, directo)
        def accion = OutboxIntegracionBase.evento("A-2")

        when:
        registro(TOPIC, true).registrar(TIPO, [id: "7"])

        then:
        thrown(OutboxSoloDrenarException)

        when:
        auditoria.registrar(accion)

        then:
        1 * directo.publicar({ AuditoriaAccionEvent e -> e.eventId() == accion.eventId() })
        leer(accion.eventId()) == null

        when:
        def grabador = new GrabadorGenerico()
        relay(grabador).ejecutarCiclo()

        then:
        grabador.publicados.size() == 1
        leer(pendiente.eventId()).estado() == EstadoOutbox.ENVIADO
        almacen().resumen().pendientes() == 0
    }

    def "métricas con ack lento: con timeout-ack de 30 s y el relay esperando el ack, los gauges se siguen actualizando"() {
        given:
        def propiedadesLentas = new AuditoriaOutboxProperties(almacen: AuditoriaOutboxProperties.Almacen.JDBC,
                tamanoLote: 10, maxIntentos: 5, backoffBase: Duration.ofSeconds(1), backoffMax: Duration.ofSeconds(10),
                lease: Duration.ofMinutes(2), timeoutAck: Duration.ofSeconds(30),
                intervalo: Duration.ofMillis(100), intervaloMetricas: Duration.ofMillis(200),
                intervaloLimpieza: Duration.ofHours(1))
        def enAck = new CountDownLatch(1)
        def liberar = new CountDownLatch(1)
        PublicadorOutbox lento = { String t, String k, String v, Map<String, String> c, Duration espera ->
            enAck.countDown()
            liberar.await(espera.toMillis(), TimeUnit.MILLISECONDS)
        } as PublicadorOutbox
        def reloj = Clock.systemUTC()
        def relay = new OutboxAuditoriaRelay(almacen(), new OutboxIntegracionBase.Grabador(), lento, propiedadesLentas,
                reloj, OutboxAuditoriaRelay.Pausa.DORMIR)
        def estadisticas = new AuditoriaOutboxEstadisticas(almacen(), reloj)
        def registroMetricas = new SimpleMeterRegistry()
        new AuditoriaOutboxMetricas(estadisticas, "ms-prueba", [TIPO] as Set).bindTo(registroMetricas)
        def limpieza = new AuditoriaOutboxLimpieza(almacen(), propiedadesLentas, BloqueoLimpieza.SIN_BLOQUEO, "ms-prueba",
                reloj)
        def planificador = new AuditoriaOutboxPlanificador(relay, limpieza, propiedadesLentas, estadisticas)
        def registroReal = new OutboxRegistro(almacen(), tipos(), "ms-prueba", reloj, false)
        def gauge = { String nombre -> registroMetricas.get(nombre).tag("tipo", TIPO).gauge().value() }

        when: "el relay toma el primer evento y se queda esperando el ack"
        registroReal.registrar(TIPO, [id: "M1"])
        planificador.start()
        def bloqueado = enAck.await(10, TimeUnit.SECONDS)
        and: "mientras tanto llegan tres eventos más"
        (2..4).each { registroReal.registrar(TIPO, [id: "M" + it]) }
        def hasta = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < hasta && gauge(AuditoriaOutboxMetricas.OUTBOX_PENDIENTES) != 4d) {
            Thread.sleep(100)
        }

        then: "los gauges reflejan los cuatro pendientes y el que está en proceso, sin esperar al ack"
        bloqueado
        liberar.count == 1
        gauge(AuditoriaOutboxMetricas.OUTBOX_PENDIENTES) == 4d
        gauge(AuditoriaOutboxMetricas.OUTBOX_EN_PROCESO) == 1d
        gauge(AuditoriaOutboxMetricas.OUTBOX_EDAD) > 0d
        registroMetricas.get(AuditoriaOutboxMetricas.PENDIENTES).tag("servicio", "ms-prueba").gauge().value() == 4d

        cleanup:
        liberar.countDown()
        planificador?.stop()
    }
}
