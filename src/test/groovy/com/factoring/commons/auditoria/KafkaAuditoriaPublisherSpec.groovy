package com.factoring.commons.auditoria

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

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
}
