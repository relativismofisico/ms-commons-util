package com.factoring.commons.outbox

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.factoring.commons.auditoria.AuditoriaAutoConfiguration
import com.factoring.commons.auditoria.KafkaAuditoriaPublisher
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxAutoConfiguration
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import spock.lang.Specification

import java.time.Duration

class ProductorOutboxSpec extends Specification {

    static final long LINGER_POR_DEFECTO = ProducerConfig.configDef().defaultValues()[ProducerConfig.LINGER_MS_CONFIG] as long

    ListAppender<ILoggingEvent> logs = new ListAppender<>()
    Logger logger = (Logger) LoggerFactory.getLogger(ProductorOutbox)

    def setup() {
        logs.start()
        logger.addAppender(logs)
    }

    def cleanup() {
        logger.detachAppender(logs)
    }

    def "deriva delivery, request y max.block de timeout-ack y cumple delivery >= linger + request"() {
        when:
        def props = ProductorOutbox.acotarTimeouts([:], ack)

        then:
        props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] == entrega
        props[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] == Math.min(30_000L, entrega - LINGER_POR_DEFECTO)
        props[ProducerConfig.MAX_BLOCK_MS_CONFIG] == (long) entrega
        (props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] as long) >=
                LINGER_POR_DEFECTO + (props[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] as long)
        (props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] as long) < ack.toMillis()

        where:
        ack                      | entrega
        Duration.ofSeconds(30)   | 27_000
        Duration.ofSeconds(3)    | 2_700
        Duration.ofMillis(500)   | 400
        Duration.ofMillis(100)   | 50
    }

    def "un valor configurado más bajo se respeta, y los valores pueden venir como texto"() {
        given:
        Map<String, Object> props = [(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG): "1500",
                                     (ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG) : 1000,
                                     (ProducerConfig.LINGER_MS_CONFIG)          : "20",
                                     (ProducerConfig.MAX_BLOCK_MS_CONFIG)       : 800L]

        when:
        ProductorOutbox.acotarTimeouts(props, Duration.ofSeconds(30))

        then:
        props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] == 1500
        props[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] == 1000
        props[ProducerConfig.MAX_BLOCK_MS_CONFIG] == 800L
    }

    def "request.timeout.ms se recorta para que quepa linger.ms dentro de delivery.timeout.ms"() {
        when:
        def props = ProductorOutbox.acotarTimeouts([(ProducerConfig.LINGER_MS_CONFIG): 500], Duration.ofSeconds(3))

        then:
        props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] == 2_700
        props[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] == 2_200
    }

    def "un timeout-ack que no deja tiempo para linger.ms es un error de configuración con un mensaje claro"() {
        when:
        ProductorOutbox.acotarTimeouts([(ProducerConfig.LINGER_MS_CONFIG): 5_000], Duration.ofSeconds(3))

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("linger.ms=5000")
        e.message.contains("timeout-ack")
    }

    def "comprobar: INFO si el plazo de entrega es menor que timeout-ack, ERROR si no, WARN si el publicador no lo informa"() {
        given:
        PublicadorOutbox publicador = Stub() {
            plazoDeEntrega() >> plazo
        }

        expect:
        ProductorOutbox.comprobar(publicador, Duration.ofSeconds(3)) == bien
        logs.list.collect { it.level } == [nivel]

        where:
        plazo                                 | bien  | nivel
        Optional.of(Duration.ofMillis(2_700)) | true  | Level.INFO
        Optional.of(Duration.ofSeconds(3))    | false | Level.ERROR
        Optional.of(Duration.ofMinutes(2))    | false | Level.ERROR
        Optional.empty()                      | false | Level.WARN
    }

    def "un PublicadorOutbox propio que no sobrescribe plazoDeEntrega no lo informa (compatibilidad)"() {
        given:
        PublicadorOutbox propio = { t, c, v, h, e -> } as PublicadorOutbox

        expect:
        propio.plazoDeEntrega() == Optional.empty()
    }

    // ---------------------------------------------------------------- autoconfiguración

    static final String[] BASE = ["spring.kafka.bootstrap-servers=localhost:1", "spring.application.name=ms-prueba",
                                  "auditoria.outbox.intervalo=1h", "auditoria.outbox.enabled=true",
                                  "auditoria.outbox.almacen=jdbc", "auditoria.outbox.timeout-ack=3s",
                                  "auditoria.outbox.lease=1m"]

    ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration,
                    AuditoriaOutboxAutoConfiguration))
            .withBean("outbox", AuditoriaOutboxPort, { Stub(AuditoriaOutboxPort) })

    static Map<String, Object> propiedadesDe(Object publicador) {
        publicador.@kafkaTemplate.producerFactory.configurationProperties
    }

    def "con el outbox activo, el productor del outbox y el de auditoría quedan acotados a timeout-ack"() {
        expect:
        runner.withPropertyValues(BASE).run { ctx ->
            assert ctx.startupFailure == null
            def outbox = ctx.getBean(PublicadorOutbox)
            assert outbox instanceof KafkaPublicadorOutbox
            assert outbox.plazoDeEntrega() == Optional.of(Duration.ofMillis(2_700))
            [propiedadesDe(outbox), propiedadesDe(ctx.getBean(KafkaAuditoriaPublisher))].each { props ->
                assert props[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] == 2_700
                assert props[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] == 2_700 - LINGER_POR_DEFECTO
                assert props[ProducerConfig.MAX_BLOCK_MS_CONFIG] == 2_700L
                assert props[ProducerConfig.ACKS_CONFIG] == "all"
                assert props[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] == true
            }
        }
    }

    def "un delivery.timeout.ms del servicio más bajo se respeta en los dos productores"() {
        expect:
        runner.withPropertyValues(BASE)
                .withPropertyValues("spring.kafka.producer.properties.delivery.timeout.ms=1500")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    [propiedadesDe(ctx.getBean(PublicadorOutbox)), propiedadesDe(ctx.getBean(KafkaAuditoriaPublisher))].each {
                        assert it[ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] == 1_500
                        assert (it[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] as long) <= 1_500 - LINGER_POR_DEFECTO
                    }
                }
    }

    def "sin el outbox, el productor de auditoría no cambia (sus timeouts siguen siendo los del servicio)"() {
        expect:
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration))
                .withPropertyValues("spring.kafka.bootstrap-servers=localhost:1", "spring.application.name=ms-prueba")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    def props = propiedadesDe(ctx.getBean(KafkaAuditoriaPublisher))
                    assert !props.containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)
                    assert !props.containsKey(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
                    assert !props.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                }
    }

    def "un PublicadorOutbox propio se respeta y el arranque avisa según el plazo de entrega que informe"() {
        given:
        PublicadorOutbox propio = Stub() {
            plazoDeEntrega() >> plazo
        }

        expect:
        runner.withPropertyValues(BASE).withBean(PublicadorOutbox, { propio }).run { ctx ->
            assert ctx.startupFailure == null
            assert ctx.getBean(PublicadorOutbox).is(propio)
            assert ctx.getBeansOfType(KafkaPublicadorOutbox).isEmpty()
        }
        logs.list.collect { it.level } == [nivel]

        where:
        plazo                                 | nivel
        Optional.empty()                      | Level.WARN
        Optional.of(Duration.ofMinutes(2))    | Level.ERROR
        Optional.of(Duration.ofMillis(2_000)) | Level.INFO
    }

    def "timeout-ack demasiado corto para el linger.ms del servicio hace fallar el arranque"() {
        expect:
        runner.withPropertyValues(BASE)
                .withPropertyValues("spring.kafka.producer.properties.linger.ms=5000")
                .run { ctx -> assert ctx.startupFailure != null }
    }
}
