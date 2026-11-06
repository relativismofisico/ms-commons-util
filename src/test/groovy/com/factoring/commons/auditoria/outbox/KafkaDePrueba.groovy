package com.factoring.commons.auditoria.outbox

import com.factoring.commons.auditoria.KafkaAuditoriaPublisher
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer

import java.time.Duration

/**
 * Contenedores de las pruebas de integración del outbox, uno de cada tipo por JVM. Se nombran con el prefijo
 * {@value #PREFIJO} (el único que estas pruebas pueden crear y borrar en una máquina de desarrollo) y se paran al
 * terminar la JVM.
 */
final class KafkaDePrueba {

    static final String PREFIJO = "integrity-e2e-tc-"

    private static KafkaContainer kafka

    private KafkaDePrueba() {
    }

    static <T extends GenericContainer<?>> T nombrado(T contenedor, String tipo) {
        contenedor.withCreateContainerCmdModifier { it.withName(PREFIJO + tipo + "-" + UUID.randomUUID().toString().substring(0, 8)) }
        contenedor
    }

    static void pararAlTerminar(GenericContainer<?> contenedor) {
        Runtime.runtime.addShutdownHook(new Thread({ contenedor.stop() }))
    }

    static synchronized KafkaContainer kafka() {
        if (kafka == null) {
            kafka = nombrado(new KafkaContainer("apache/kafka:3.8.1"), "kafka")
            kafka.start()
            pararAlTerminar(kafka)
        }
        kafka
    }

    /** Publicador real, con timeouts cortos para que un broker pausado falle rápido. */
    static KafkaAuditoriaPublisher publicador(String topic) {
        Map<String, Object> props = [
                (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)     : kafka().bootstrapServers,
                (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)  : StringSerializer,
                (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG): StringSerializer,
                (ProducerConfig.ACKS_CONFIG)                  : "all",
                (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)    : true,
                (ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)    : 2000,
                (ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)   : 4000,
                (ProducerConfig.LINGER_MS_CONFIG)             : 0,
                (ProducerConfig.MAX_BLOCK_MS_CONFIG)          : 3000]
        new KafkaAuditoriaPublisher(new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(props)), topic,
                "ms-prueba")
    }

    /** Claves (eventId) leídas del topic desde el principio hasta que no llega nada durante un rato. */
    static List<String> eventIdsPublicados(String topic, int esperados) {
        Map<String, Object> props = [
                (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)       : kafka().bootstrapServers,
                (ConsumerConfig.GROUP_ID_CONFIG)                : "lector-" + UUID.randomUUID(),
                (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)       : "earliest",
                (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)  : StringDeserializer,
                (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer]
        def ids = []
        new KafkaConsumer<String, String>(props).withCloseable { consumidor ->
            consumidor.subscribe([topic])
            long hasta = System.currentTimeMillis() + 30_000
            while (System.currentTimeMillis() < hasta && ids.size() < esperados) {
                consumidor.poll(Duration.ofMillis(500)).each { ids << com.factoring.commons.auditoria.AuditoriaJson.deserializar(it.value()).eventId() }
            }
        }
        ids
    }

    static void pausar() {
        DockerClientFactory.instance().client().pauseContainerCmd(kafka().containerId).exec()
    }

    static void reanudar() {
        DockerClientFactory.instance().client().unpauseContainerCmd(kafka().containerId).exec()
    }
}
