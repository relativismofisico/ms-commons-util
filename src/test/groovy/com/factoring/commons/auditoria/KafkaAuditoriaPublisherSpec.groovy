package com.factoring.commons.auditoria

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class KafkaAuditoriaPublisherSpec extends Specification {

    KafkaTemplate<String, String> template = Mock()
    KafkaAuditoriaPublisher publisher = new KafkaAuditoriaPublisher(template, "auditoria.accion", "ms-prueba")

    private static AuditoriaAccionEvent evento(String servicio = null) {
        AuditoriaAccionEvent.builder()
                .servicioOrigen(servicio)
                .accion(Acciones.FACTURA_CARGADA)
                .entidad(EntidadTipo.FACTURA, "CUFE-1")
                .actor(AuditoriaActor.usuario("u1", "EMPRESA", 1L))
                .canal(Canal.WEB)
                .build()
    }

    def "publica en el topic con clave <tipo>:<id> y JSON; completa servicioOrigen si falta"() {
        given:
        def e = evento()
        String enviado = null

        when:
        publisher.publicar(e)

        then:
        1 * template.send("auditoria.accion", "FACTURA:CUFE-1", _ as String) >> { args ->
            enviado = args[2]
            CompletableFuture.completedFuture(Mock(SendResult))
        }
        def leido = AuditoriaJson.deserializar(enviado)
        leido.servicioOrigen() == "ms-prueba"
        leido.eventId() == e.eventId()
    }

    def "respeta el servicioOrigen que informó el productor"() {
        when:
        publisher.publicar(evento("ms-otro"))

        then:
        1 * template.send(_, _, { it.contains('"servicioOrigen":"ms-otro"') }) >> CompletableFuture.completedFuture(Mock(SendResult))
    }

    def "un fallo de Kafka (asíncrono o síncrono) no se propaga: la acción de negocio ya está guardada"() {
        when:
        publisher.publicar(evento())

        then:
        1 * template.send(_, _, _) >> respuesta
        noExceptionThrown()

        where:
        respuesta << [CompletableFuture.failedFuture(new RuntimeException("broker caído")),
                      { throw new IllegalStateException("max.block.ms") }]
    }

    def "destroy cierra el template propio"() {
        when:
        publisher.destroy()

        then:
        1 * template.destroy()
    }

    // ── publicarConfirmado (relay de outbox) ──────────────────────────────────

    def "publicarConfirmado espera la confirmación del broker, con la misma clave y JSON"() {
        given:
        def e = evento()

        when:
        publisher.publicarConfirmado(e)

        then:
        1 * template.send("auditoria.accion", "FACTURA:CUFE-1", { AuditoriaJson.deserializar(it).eventId() == e.eventId() &&
                AuditoriaJson.deserializar(it).servicioOrigen() == "ms-prueba" }) >> CompletableFuture.completedFuture(Mock(SendResult))
        noExceptionThrown()
    }

    def "publicarConfirmado propaga el fallo de Kafka (asíncrono o síncrono) para que el outbox reintente"() {
        given:
        def e = evento()

        when:
        publisher.publicarConfirmado(e)

        then:
        1 * template.send(_, _, _) >> respuesta
        def error = thrown(AuditoriaNoPublicadaException)
        error.message.contains(e.eventId())

        where:
        respuesta << [CompletableFuture.failedFuture(new RuntimeException("broker caído")),
                      { throw new IllegalStateException("max.block.ms") }]
    }

    def "publicarConfirmado interrumpido: restaura el flag de interrupción y propaga"() {
        given:
        def futuro = Mock(CompletableFuture) {
            get(_, _) >> { throw new InterruptedException("apagando") }
        }

        when:
        publisher.publicarConfirmado(evento())

        then:
        1 * template.send(_, _, _) >> futuro
        thrown(AuditoriaNoPublicadaException)
        Thread.interrupted()
    }

    def "publicarConfirmado con topic y espera: publica en ese topic y espera solo lo indicado"() {
        given:
        def futuro = Mock(CompletableFuture)

        when:
        publisher.publicarConfirmado(evento(), "auditoria.otro", Duration.ofMillis(1500))

        then:
        1 * template.send("auditoria.otro", "FACTURA:CUFE-1", _ as String) >> futuro
        1 * futuro.get(1500L, TimeUnit.MILLISECONDS)
    }

    def "publicarConfirmado sin topic usa el configurado; un timeout del ack se propaga"() {
        given:
        def futuro = Mock(CompletableFuture) {
            get(_, _) >> { throw new TimeoutException() }
        }

        when:
        publisher.publicarConfirmado(evento(), topic, Duration.ofSeconds(1))

        then:
        1 * template.send("auditoria.accion", _, _) >> futuro
        thrown(AuditoriaNoPublicadaException)

        where:
        topic << [null, " "]
    }

    def "la implementación por defecto de la interfaz delega en publicarConfirmado(evento)"() {
        given:
        def llamadas = []
        AuditoriaPublisher propio = new AuditoriaPublisher() {
            void publicar(AuditoriaAccionEvent e) {}
            void publicarConfirmado(AuditoriaAccionEvent e) { llamadas << e.eventId() }
        }
        def e = evento()

        when:
        propio.publicarConfirmado(e, "auditoria.otro", Duration.ofSeconds(1))

        then:
        llamadas == [e.eventId()]
    }

    def "evento no publicado: el log general solo lleva identificadores; el evento completo va al logger dedicado"() {
        given:
        def general = capturar(KafkaAuditoriaPublisher.name)
        def dedicado = capturar(KafkaAuditoriaPublisher.LOGGER_NO_PUBLICADO)
        def sensible = AuditoriaAccionEvent.builder()
                .accion(Acciones.FACTURA_CARGADA)
                .entidad(EntidadTipo.FACTURA, "CUFE-1")
                .actor(AuditoriaActor.usuario("actor-sensible", "EMPRESA", 1L))
                .canal(Canal.WEB)
                .ip("203.0.113.77")
                .detalle("monto", "detalle-sensible-123")
                .build()

        when:
        publisher.publicar(sensible)

        then:
        1 * template.send(_, _, _) >> CompletableFuture.failedFuture(new RuntimeException("broker caído"))
        general.list.size() == 1
        def lineaGeneral = general.list[0].formattedMessage
        lineaGeneral.contains("eventId=" + sensible.eventId())
        lineaGeneral.contains("accion=" + Acciones.FACTURA_CARGADA)
        lineaGeneral.contains("entidad=" + EntidadTipo.FACTURA + ":CUFE-1")
        lineaGeneral.contains("servicioOrigen=ms-prueba")
        !lineaGeneral.contains("203.0.113.77")
        !lineaGeneral.contains("actor-sensible")
        !lineaGeneral.contains("detalle-sensible-123")

        dedicado.list.size() == 1
        def lineaDedicada = dedicado.list[0].formattedMessage
        lineaDedicada.contains(sensible.eventId())
        lineaDedicada.contains("203.0.113.77")
        lineaDedicada.contains("actor-sensible")
        lineaDedicada.contains("detalle-sensible-123")

        cleanup:
        soltar(KafkaAuditoriaPublisher.name, general)
        soltar(KafkaAuditoriaPublisher.LOGGER_NO_PUBLICADO, dedicado)
    }

    def "el logger dedicado tiene un nombre fijo: los logback de los servicios lo referencian por nombre"() {
        expect:
        KafkaAuditoriaPublisher.LOGGER_NO_PUBLICADO == "AUDITORIA_NO_PUBLICADO"
    }

    private static ListAppender<ILoggingEvent> capturar(String nombre) {
        def appender = new ListAppender<ILoggingEvent>()
        appender.start()
        ((Logger) LoggerFactory.getLogger(nombre)).addAppender(appender)
        appender
    }

    private static void soltar(String nombre, ListAppender<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(nombre)).detachAppender(appender)
    }
}
