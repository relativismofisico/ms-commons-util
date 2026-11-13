package com.factoring.commons.outbox

import com.factoring.commons.auditoria.AuditoriaAutoConfiguration
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxAutoConfiguration
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.KafkaDePrueba
import com.factoring.commons.auditoria.outbox.OutboxAuditoriaRelay
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.kafka.KafkaContainer
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

import java.time.Duration

/**
 * Caída del broker más larga que {@code timeout-ack}: cada intento que vence tiene que quedar definitivamente fallido
 * (el productor descarta el registro), de modo que al volver el broker salga exactamente un mensaje por evento.
 *
 * <p>El broker se PARA (conexión rechazada), no se pausa: con un contenedor pausado el sistema operativo sigue aceptando
 * los bytes y un lote ya enviado puede escribirse al reanudar; ese caso (resultado desconocido de un envío en vuelo) no
 * lo puede evitar ningún productor y está documentado en el README. Se usa un broker propio con puerto fijo para poder
 * pararlo y arrancarlo sin que cambie la dirección anunciada.</p>
 */
@Stepwise
class OutboxCaidaDelBrokerIntegracionSpec extends Specification {

    static final String TIPO = "documento.creado"
    static final String TABLA = "outbox_caida"

    @Shared
    int puerto = puertoLibre()

    @Shared
    KafkaContainer kafka = KafkaDePrueba.nombrado(new KafkaContainer("apache/kafka:3.8.1"), "kafka-caida")
            .withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig.withPortBindings(new PortBinding(Ports.Binding.bindPort(puerto), new ExposedPort(9092)))
            }

    @Shared
    MySQLContainer<?> mysql = KafkaDePrueba.nombrado(new MySQLContainer<>("mysql:8.0"), "mysql-caida")

    @Shared
    HikariDataSource dataSource

    @Shared
    JdbcAuditoriaOutboxAdapter almacen

    def setupSpec() {
        kafka.start()
        mysql.start()
        dataSource = new HikariDataSource(jdbcUrl: mysql.jdbcUrl, username: mysql.username, password: mysql.password,
                maximumPoolSize: 5)
        def jdbc = new JdbcTemplate(dataSource)
        jdbc.execute(getClass().getResource("/outbox_mysql.sql").text.replace("outbox_ms_prueba", TABLA))
        jdbc.execute(getClass().getResource("/auditoria_outbox_mysql.sql").text)
        almacen = new JdbcAuditoriaOutboxAdapter(dataSource, TABLA)
    }

    def cleanupSpec() {
        dataSource?.close()
        mysql.stop()
        kafka.stop()
    }

    static int puertoLibre() {
        new ServerSocket(0).withCloseable { it.localPort }
    }

    ApplicationContextRunner contexto(String topic, Duration timeoutAck) {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration,
                        AuditoriaOutboxAutoConfiguration))
                .withPropertyValues("spring.kafka.bootstrap-servers=" + kafka.bootstrapServers,
                        "spring.application.name=ms-prueba",
                        "auditoria.outbox.enabled=true", "auditoria.outbox.almacen=jdbc",
                        "auditoria.outbox.intervalo=1h", "auditoria.outbox.intervalo-metricas=1h",
                        "auditoria.outbox.timeout-ack=" + timeoutAck.toMillis() + "ms",
                        "auditoria.outbox.lease=" + timeoutAck.multipliedBy(20).toMillis() + "ms",
                        "auditoria.outbox.max-intentos=50",
                        "auditoria.outbox.backoff-base=200ms", "auditoria.outbox.backoff-max=200ms")
                .withBean("outbox", AuditoriaOutboxPort, { almacen })
                .withBean(TiposDeEvento, {
                    TiposDeEvento.de(TipoDeEvento.de(TIPO, Map).topic(topic).clave({ Map m -> m.clave as String }).build())
                })
    }

    void pararBroker() {
        DockerClientFactory.instance().client().stopContainerCmd(kafka.containerId).withTimeout(10).exec()
    }

    void arrancarBroker() {
        DockerClientFactory.instance().client().startContainerCmd(kafka.containerId).exec()
        esperarBroker()
    }

    /** Un productor aparte publica en un topic de control hasta que el broker vuelve a confirmar. */
    void esperarBroker() {
        Map<String, Object> props = [
                (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)     : kafka.bootstrapServers,
                (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)  : StringSerializer,
                (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG): StringSerializer,
                (ProducerConfig.MAX_BLOCK_MS_CONFIG)          : 2000,
                (ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)    : 2000,
                (ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)   : 3000]
        def template = new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory<String, String>(props))
        try {
            long hasta = System.currentTimeMillis() + 90_000
            while (true) {
                try {
                    template.send("control", "x").get(5, java.util.concurrent.TimeUnit.SECONDS)
                    return
                } catch (Exception e) {
                    if (System.currentTimeMillis() > hasta) {
                        throw new IllegalStateException("el broker no volvió", e)
                    }
                    sleep(500)
                }
            }
        } finally {
            template.destroy()
        }
    }

    /** eventId de cada mensaje del topic, leído desde el principio hasta que no llega nada durante un rato. */
    Map<String, Integer> copiasPorEventId(String topic) {
        Map<String, Object> props = [
                (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)       : kafka.bootstrapServers,
                (ConsumerConfig.GROUP_ID_CONFIG)                : "lector-" + UUID.randomUUID(),
                (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)       : "earliest",
                (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)  : StringDeserializer,
                (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer]
        Map<String, Integer> copias = [:].withDefault { 0 }
        new KafkaConsumer<String, String>(props).withCloseable { consumidor ->
            consumidor.subscribe([topic])
            long ultimo = System.currentTimeMillis()
            long hasta = ultimo + 60_000
            while (System.currentTimeMillis() < hasta && System.currentTimeMillis() - ultimo < 8_000) {
                def leidos = consumidor.poll(Duration.ofMillis(500))
                leidos.each { r ->
                    def cabecera = r.headers().lastHeader(EventoOutbox.CABECERA_EVENT_ID)
                    copias[new String(cabecera.value(), "UTF-8")]++
                }
                if (!leidos.isEmpty()) {
                    ultimo = System.currentTimeMillis()
                }
            }
        }
        copias
    }

    /** Ciclos del relay hasta que no quede nada pendiente (o se agote el plazo). */
    static void drenar(OutboxAuditoriaRelay relay, AuditoriaOutboxPort almacen, long plazoMs) {
        long hasta = System.currentTimeMillis() + plazoMs
        while (System.currentTimeMillis() < hasta) {
            relay.ejecutarCiclo()
            if (almacen.resumen().pendientes() == 0) {
                return
            }
            sleep(300)
        }
    }

    def "caída del broker más larga que timeout-ack con varios eventos y claves: al volver sale exactamente un mensaje por evento"() {
        given:
        def topic = "caida-larga-" + UUID.randomUUID().toString().substring(0, 8)
        Map<String, Integer> copias = null
        List<String> eventIds = []
        long fallosDuranteLaCaida = 0

        when:
        contexto(topic, Duration.ofSeconds(3)).run { ctx ->
            assert ctx.startupFailure == null
            def puerto = ctx.getBean(OutboxPort)
            def relay = ctx.getBean(OutboxAuditoriaRelay)
            // con el broker arriba: el productor del outbox ya tiene la metadata del topic, como un servicio en marcha
            puerto.registrar(TIPO, [id: "previo", clave: "K0"])
            drenar(relay, almacen, 30_000)
            pararBroker()
            ["A-1": "K1", "A-2": "K1", "B-1": "K2", "C-1": "K3"].each { id, clave ->
                eventIds << puerto.registrar(TIPO, [id: id, clave: clave]).eventId()
            }
            // al menos dos intentos vencidos por evento mientras el broker está parado
            3.times { relay.ejecutarCiclo(); sleep(400) }
            fallosDuranteLaCaida = almacen.resumen().pendientes()
            arrancarBroker()
            drenar(relay, almacen, 60_000)
            copias = copiasPorEventId(topic)
        }

        then: "los cuatro eventos fallaron durante la caída y se entregaron una sola vez al volver"
        fallosDuranteLaCaida == 4
        eventIds.collect { copias[it] } == [1, 1, 1, 1]
        copias.values().sum() == 5
    }

    def "caída corta (menor que timeout-ack): el intento en curso se confirma al volver el broker y sale un solo mensaje"() {
        given:
        def topic = "caida-corta-" + UUID.randomUUID().toString().substring(0, 8)
        Map<String, Integer> copias = null
        String eventId = null

        when:
        contexto(topic, Duration.ofSeconds(60)).run { ctx ->
            assert ctx.startupFailure == null
            def puerto = ctx.getBean(OutboxPort)
            def relay = ctx.getBean(OutboxAuditoriaRelay)
            puerto.registrar(TIPO, [id: "previo", clave: "K0"])
            drenar(relay, almacen, 30_000)
            pararBroker()
            eventId = puerto.registrar(TIPO, [id: "corta", clave: "K9"]).eventId()
            def vuelta = Thread.start { sleep(2000); arrancarBroker() }
            relay.ejecutarCiclo()
            vuelta.join()
            drenar(relay, almacen, 60_000)
            copias = copiasPorEventId(topic)
        }

        then:
        copias[eventId] == 1
        copias.values().sum() == 2
    }

    /** p95 en ms de {@code n} envíos confirmados (tras calentar el productor). */
    long p95(PublicadorOutbox publicador, String topic, int n) {
        20.times { publicador.publicarConfirmado(topic, "k", "{}", [eventId: "c-" + it], Duration.ofSeconds(30)) }
        List<Long> tiempos = (0..<n).collect { i ->
            long t0 = System.nanoTime()
            publicador.publicarConfirmado(topic, "k" + (i % 7), "{}", [eventId: "m-" + i], Duration.ofSeconds(30))
            (System.nanoTime() - t0).intdiv(1_000_000L) as long
        }.sort()
        tiempos[(int) Math.ceil(n * 0.95) - 1]
    }

    def "broker disponible: acotar los timeouts no cambia la latencia de la confirmación (p95 con el mismo escenario)"() {
        given:
        def topic = "latencia-" + UUID.randomUUID().toString().substring(0, 8)
        Map<String, Object> base = [
                (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)     : kafka.bootstrapServers,
                (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)  : StringSerializer,
                (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG): StringSerializer,
                (ProducerConfig.ACKS_CONFIG)                  : "all",
                (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)    : true]
        def sinAcotar = new KafkaPublicadorOutbox(new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(new HashMap<>(base))))
        def acotado = new KafkaPublicadorOutbox(new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(
                ProductorOutbox.acotarTimeouts(new HashMap<>(base), Duration.ofSeconds(30)))))

        when:
        long antes = p95(sinAcotar, topic, 300)
        long despues = p95(acotado, topic, 300)
        println "[LATENCIA] p95 sin acotar=${antes} ms, acotado=${despues} ms (300 envios cada uno)"

        then:
        despues <= antes * 2 + 10

        cleanup:
        sinAcotar?.destroy()
        acotado?.destroy()
    }
}
