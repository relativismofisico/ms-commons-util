package com.factoring.commons.outbox

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.EstadoOutbox
import com.factoring.commons.mdc.MdcConstants
import org.slf4j.MDC
import spock.lang.Specification

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/** Declaración de tipos (fail-fast) y registro de eventos genéricos con un almacén simulado. */
class TiposDeEventoSpec extends Specification {

    static TipoDeEvento<Map> documento(String nombre = "documento.creado", String topic = "documentos") {
        TipoDeEvento.de(nombre, Map).version(2).topic(topic).clave({ Map m -> m.id == null ? null : "DOC:" + m.id }).build()
    }

    def "una declaración válida: nombres, versión, topic resuelto y búsqueda por nombre"() {
        when:
        def tipos = TiposDeEvento.de(documento(), documento("documento.borrado", '${topics.borrado}'))
                .resolver { it == '${topics.borrado}' ? "documentos.borrados" : it }

        then:
        tipos.nombres() == ["documento.creado", "documento.borrado"] as Set
        tipos.requerir("documento.borrado").topic() == "documentos.borrados"
        tipos.requerir("documento.creado").version() == 2
    }

    def "declaraciones inválidas fallan al arrancar con el motivo"() {
        when:
        TiposDeEvento.de(tipos as TipoDeEvento[])

        then:
        def e = thrown(IllegalStateException)
        e.message.contains(motivo)

        where:
        tipos                                                                              || motivo
        []                                                                                 || "no hay tipos"
        [documento(), documento()]                                                         || "declarado dos veces"
        [TipoDeEvento.de(" ", Map).topic("t").clave({ "k" }).build()]                      || "sin nombre"
        [TipoDeEvento.de("a", Map).version(0).topic("t").clave({ "k" }).build()]           || "versión 1 o más"
        [TipoDeEvento.de("a", Map).clave({ "k" }).build()]                                 || "no tiene topic"
        [TipoDeEvento.de("a", Map).topic("t").build()]                                     || "no tiene función de clave"
    }

    def "un topic que no se puede resolver hace fallar el arranque"() {
        when:
        TiposDeEvento.de(documento("a", '${topics.no.existe}')).resolver(resolver)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("topic del tipo a")

        where:
        resolver << [{ String t -> throw new IllegalArgumentException("sin propiedad") }, { String t -> t }, { String t -> "" }]
    }

    def "un tipo no declarado se rechaza en ejecución"() {
        when:
        TiposDeEvento.de(documento()).requerir("otro.tipo")

        then:
        def e = thrown(TipoDeEventoNoDeclaradoException)
        e.message.contains("otro.tipo")
    }

    def "registrar: sobre con eventId nuevo, tipo, versión, servicio, hora, requestId del MDC y clave de la función"() {
        given:
        def guardados = []
        AuditoriaOutboxPort almacen = Stub() { guardar(_) >> { args -> guardados << args[0] } }
        def ahora = Instant.parse("2026-11-08T10:00:00.123456Z")
        def registro = new OutboxRegistro(almacen, TiposDeEvento.de(documento()), "ms-prueba",
                Clock.fixed(ahora, ZoneOffset.UTC), false)
        MDC.put(MdcConstants.REQUEST_ID, "req-1")

        when:
        def primero = registro.registrar("documento.creado", [id: "7", titulo: "uno"])
        def segundo = registro.registrar("documento.creado", [id: "7", titulo: "dos"])

        then:
        primero.eventId() != segundo.eventId()
        UUID.fromString(primero.eventId())
        primero.tipo() == "documento.creado"
        primero.version() == 2
        primero.servicioOrigen() == "ms-prueba"
        primero.ocurridoEn() == Instant.parse("2026-11-08T10:00:00.123Z")
        primero.requestId() == "req-1"
        primero.clave() == "DOC:7"
        guardados.size() == 2
        with(guardados[0] as AuditoriaOutboxEvento) {
            eventId() == primero.eventId()
            claveKafka() == "DOC:7"
            topic() == "documentos"
            tipo() == "documento.creado"
            estado() == EstadoOutbox.PENDIENTE
            OutboxJson.deserializar(payload(), Map) == primero
        }
        OutboxJson.cabeceras(guardados[0].payload()) == [eventId: primero.eventId(), tipo: "documento.creado",
                                                         version: "2", servicioOrigen: "ms-prueba"]

        cleanup:
        MDC.remove(MdcConstants.REQUEST_ID)
    }

    def "registrar rechaza sin escribir: tipo no declarado, payload de otra clase, clave vacía y modo solo drenar"() {
        given:
        AuditoriaOutboxPort almacen = Mock()
        def registro = new OutboxRegistro(almacen, TiposDeEvento.de(documento()), "ms-prueba", Clock.systemUTC(),
                soloDrenar)

        when:
        registro.registrar(tipo, payload)

        then:
        thrown(excepcion)
        0 * almacen.guardar(_)

        where:
        tipo               | payload     | soloDrenar || excepcion
        "otro.tipo"        | [id: "1"]   | false      || TipoDeEventoNoDeclaradoException
        "documento.creado" | "no es map" | false      || IllegalArgumentException
        "documento.creado" | [id: null]  | false      || IllegalArgumentException
        "documento.creado" | [id: "1"]   | true       || OutboxSoloDrenarException
    }

    def "cabeceras de un sobre ilegible o incompleto: error claro"() {
        when:
        OutboxJson.cabeceras(json)

        then:
        thrown(IllegalArgumentException)

        where:
        json << ["no es json", '{"tipo":"a"}', '{"eventId":"x"}']
    }
}
