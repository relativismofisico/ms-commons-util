package com.factoring.commons.auditoria

import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import org.slf4j.MDC
import spock.lang.Specification

import java.time.Instant

class AuditoriaAccionEventSpec extends Specification {

    def cleanup() {
        MDC.clear()
    }

    private static AuditoriaAccionEventBuilder minimo() {
        AuditoriaAccionEvent.builder()
                .accion(Acciones.EMPRESA_ESTADO_CAMBIADO)
                .entidad(EntidadTipo.EMPRESA, 7600L)
                .actor(AuditoriaActor.usuario("admin1", "ADMINISTRADOR", null))
                .canal(Canal.WEB)
    }

    def "el builder genera eventId, occurredAt y schemaVersion, y toma el requestId del MDC del hilo"() {
        given:
        MDC.put("requestId", "req-1")

        when:
        def evento = minimo().estados(10L, 13L).detalle("motivo", "ok").build()

        then:
        evento.eventId() ==~ /[0-9a-f-]{36}/
        evento.schemaVersion() == 1
        evento.occurredAt() != null
        evento.requestId() == "req-1"
        evento.entidad().id() == "7600"
        evento.estadoAnterior() == "10"
        evento.estadoNuevo() == "13"
        evento.detalle() == [motivo: "ok"]
        evento.claveKafka() == "EMPRESA:7600"
    }

    def "cada evento tiene su propio eventId"() {
        expect:
        minimo().build().eventId() != minimo().build().eventId()
    }

    def "faltan campos obligatorios -> NullPointerException con el campo: #campo"() {
        when:
        construir.call()

        then:
        def e = thrown(NullPointerException)
        e.message.contains(campo)

        where:
        campo       | construir
        "accion"    | { AuditoriaAccionEvent.builder().entidad("EMPRESA", 1).actor(AuditoriaActor.anonimo()).canal("WEB").build() }
        "entidad"   | { AuditoriaAccionEvent.builder().accion("X").actor(AuditoriaActor.anonimo()).canal("WEB").build() }
        "entidad.id"| { AuditoriaAccionEvent.builder().accion("X").entidad("EMPRESA", null).actor(AuditoriaActor.anonimo()).canal("WEB").build() }
        "actor"     | { AuditoriaAccionEvent.builder().accion("X").entidad("EMPRESA", 1).canal("WEB").build() }
        "canal"     | { AuditoriaAccionEvent.builder().accion("X").entidad("EMPRESA", 1).actor(AuditoriaActor.anonimo()).build() }
    }

    def "detalle rechaza claves que parecen secretos: '#clave'"() {
        when:
        minimo().detalle(clave, "x")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(clave)

        where:
        clave << ["otp", "codigoOtp", "password", "passwordHash", "tokenAcceso", "secretoCliente", "nuevaContrasena", "clave"]
    }

    def "detalle omite valores null y queda inmutable"() {
        when:
        def evento = minimo().detalle("observacion", null).detalle("valor", 10).build()
        evento.detalle().put("otro", 1)

        then:
        thrown(UnsupportedOperationException)
        evento.detalle() == [valor: 10]
    }

    def "sin detalle, el campo queda null"() {
        expect:
        minimo().build().detalle() == null
    }

    def "conServicioOrigen devuelve una copia con el mismo eventId"() {
        given:
        def evento = minimo().build()

        when:
        def copia = evento.conServicioOrigen("ms-x")

        then:
        copia.servicioOrigen() == "ms-x"
        copia.eventId() == evento.eventId()
    }

    def "JSON: ida y vuelta sin pérdida, fechas ISO-8601 y sin headers de tipo"() {
        given:
        def evento = minimo().servicioOrigen("ms-registroempresa-neg").occurredAt(Instant.parse("2026-10-13T15:00:00Z"))
                .ip("10.0.0.1").requestId("req-9").detalle("monto", 1500).build()

        when:
        def json = AuditoriaJson.serializar(evento)
        def leido = AuditoriaJson.deserializar(json)

        then:
        json.contains('"occurredAt":"2026-10-13T15:00:00Z"')
        !json.contains('@class')
        leido == evento
    }

    def "JSON: tolera campos desconocidos (productor más nuevo que el consumidor)"() {
        given:
        def json = AuditoriaJson.serializar(minimo().build()).replaceFirst(/^\{/, '{"campoNuevo":1,')

        expect:
        AuditoriaJson.deserializar(json).accion() == Acciones.EMPRESA_ESTADO_CAMBIADO
        AuditoriaJson.mapper().readValue(json, AuditoriaAccionEvent).accion() == Acciones.EMPRESA_ESTADO_CAMBIADO
    }

    def "JSON: un actor inválido (USUARIO sin id) no se puede leer"() {
        given:
        def json = AuditoriaJson.serializar(minimo().build()).replace('"id":"admin1"', '"id":null')

        when:
        AuditoriaJson.deserializar(json)

        then:
        def e = thrown(IllegalArgumentException)
        e.cause instanceof ValueInstantiationException
    }

    def "serializar un detalle no serializable -> IllegalStateException con el eventId"() {
        given:
        def evento = minimo().detalle("raro", new Object()).build()

        when:
        AuditoriaJson.serializar(evento)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains(evento.eventId())
    }
}
