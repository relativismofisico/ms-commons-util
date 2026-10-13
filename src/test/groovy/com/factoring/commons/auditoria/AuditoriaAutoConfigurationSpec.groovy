package com.factoring.commons.auditoria

import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import spock.lang.Specification

class AuditoriaAutoConfigurationSpec extends Specification {

    ApplicationContextRunner contexto = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration))
            .withPropertyValues("spring.kafka.bootstrap-servers=localhost:1", "spring.application.name=ms-prueba")

    def "con Kafka registra el publicador sin exponer un ProducerFactory ni un KafkaTemplate extra"() {
        expect:
        contexto.run { ctx ->
            assert ctx.getBeansOfType(AuditoriaPublisher).size() == 1
            // los de KafkaAutoConfiguration del servicio siguen siendo únicos: no hay ambigüedad por tipo
            assert ctx.getBeansOfType(ProducerFactory).size() == 1
            assert ctx.getBeansOfType(KafkaTemplate).size() == 1
        }
    }

    def "auditoria.enabled=false lo apaga"() {
        expect:
        contexto.withPropertyValues("auditoria.enabled=false").run { ctx ->
            assert ctx.getBeansOfType(AuditoriaPublisher).isEmpty()
        }
    }

    def "si el servicio define su propio AuditoriaPublisher (p. ej. vía outbox), la autoconfiguración no lo reemplaza"() {
        given:
        AuditoriaPublisher propio = Mock()

        expect:
        contexto.withBean("propio", AuditoriaPublisher, { propio }).run { ctx ->
            assert ctx.getBeansOfType(AuditoriaPublisher).values() as List == [propio]
        }
    }

    def "sin spring-kafka en el classpath no registra nada"() {
        expect:
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AuditoriaAutoConfiguration))
                .withClassLoader(new FilteredClassLoader(KafkaTemplate))
                .run { ctx -> assert ctx.getBeansOfType(AuditoriaPublisher).isEmpty() }
    }
}
