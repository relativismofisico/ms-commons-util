package com.factoring.commons.auditoria.outbox

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.factoring.commons.auditoria.Acciones
import com.factoring.commons.auditoria.AuditoriaAccionEvent
import com.factoring.commons.auditoria.AuditoriaActor
import com.factoring.commons.auditoria.AuditoriaJson
import com.factoring.commons.auditoria.AuditoriaNoPublicadaException
import com.factoring.commons.auditoria.AuditoriaPublisher
import com.factoring.commons.auditoria.Canal
import com.factoring.commons.auditoria.EntidadTipo
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class OutboxAuditoriaRelaySpec extends Specification {

    static final String DATO_SENSIBLE = "detalle-que-no-debe-salir-del-payload"

    AuditoriaOutboxPort outbox = Mock()
    AuditoriaPublisher publicador = Mock()
    RelojDePrueba reloj = new RelojDePrueba()
    AuditoriaOutboxProperties propiedades = new AuditoriaOutboxProperties(
            almacen: AuditoriaOutboxProperties.Almacen.JDBC, maxIntentos: 3, backoffBase: Duration.ofSeconds(5),
            backoffMax: Duration.ofSeconds(60), timeoutAck: Duration.ofSeconds(10), lease: Duration.ofMinutes(5))
    OutboxAuditoriaRelay relay = new OutboxAuditoriaRelay(outbox, publicador, propiedades, reloj)

    ListAppender<ILoggingEvent> logs = new ListAppender<>()
    Logger logger = (Logger) LoggerFactory.getLogger(OutboxAuditoriaRelay)

    def setup() {
        logs.start()
        logger.addAppender(logs)
    }

    def cleanup() {
        logger.detachAppender(logs)
    }

    private AuditoriaOutboxEvento reclamado(String id, String entidadId, int intentos = 0) {
        AuditoriaAccionEvent accion = AuditoriaAccionEvent.builder()
                .servicioOrigen("ms-prueba")
                .accion(Acciones.FACTURA_CARGADA)
                .entidad(EntidadTipo.FACTURA, entidadId)
                .actor(AuditoriaActor.usuario("u1", "EMPRESA", 1L))
                .canal(Canal.WEB)
                .detalle("observacion", DATO_SENSIBLE)
                .build()
        new AuditoriaOutboxEvento(id, accion.eventId(), accion.claveKafka(), "auditoria.accion",
                AuditoriaJson.serializar(accion), EstadoOutbox.EN_PROCESO, intentos, reloj.instant(),
                reloj.instant().plus(propiedades.lease), reloj.instant(), null, null)
    }

    private String mensajes() {
        logs.list*.formattedMessage.join("\n")
    }

    def "lote vacío: no publica ni marca nada"() {
        when:
        def enviados = relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, 50) >> []
        1 * outbox.tomarLote(50, reloj.instant(), reloj.instant().plus(Duration.ofMinutes(5))) >> []
        0 * publicador._
        0 * outbox._
        enviados == 0
    }

    def "éxito: publica con topic y timeout del ack y marca ENVIADO solo después del ack"() {
        given:
        def evento = reclamado("1", "A")

        when:
        def enviados = relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [evento]

        then:
        1 * publicador.publicarConfirmado({ it.eventId() == evento.eventId() }, "auditoria.accion",
                Duration.ofSeconds(10))

        then:
        1 * outbox.marcarEnviado("1", reloj.instant())
        enviados == 1
    }

    def "fallo: reintento con backoff exponencial, sin marcar enviado"() {
        given:
        def evento = reclamado("1", "A", intentosPrevios)

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [evento]
        1 * publicador.publicarConfirmado(_, _, _) >> { throw new AuditoriaNoPublicadaException(evento.eventId(), new RuntimeException("x")) }
        1 * outbox.marcarReintento(evento, intentosPrevios + 1, reloj.instant().plus(espera), _ as String) >> true
        0 * outbox.marcarEnviado(_, _)

        where:
        intentosPrevios | espera
        0               | Duration.ofSeconds(5)
        1               | Duration.ofSeconds(10)
    }

    def "backoff: base * 2^(intentos-1) con tope en backoff-max"() {
        expect:
        relay.backoff(intentos) == espera

        where:
        intentos | espera
        1        | Duration.ofSeconds(5)
        2        | Duration.ofSeconds(10)
        4        | Duration.ofSeconds(40)
        5        | Duration.ofSeconds(60)
        60       | Duration.ofSeconds(60)
    }

    def "agotar el tope de intentos lo pasa a FALLIDO con log error sin payload"() {
        given:
        def evento = reclamado("7", "A", 2)

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [evento]
        1 * publicador.publicarConfirmado(_, _, _) >> { throw new AuditoriaNoPublicadaException(evento.eventId(), new IllegalStateException(DATO_SENSIBLE)) }
        1 * outbox.marcarFallido(evento, 3, _ as String) >> true
        0 * outbox.marcarReintento(*_)
        def error = logs.list.find { it.level.toString() == "ERROR" }
        error.formattedMessage.contains("OUTBOX_FALLIDO")
        error.formattedMessage.contains(evento.eventId())
        !mensajes().contains(DATO_SENSIBLE)
        !mensajes().contains(evento.payload())
    }

    def "el error guardado está saneado: tipos de excepción, sin mensajes ni payload"() {
        given:
        def evento = reclamado("1", "A")
        String guardado = null

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [evento]
        1 * publicador.publicarConfirmado(_, _, _) >> {
            throw new AuditoriaNoPublicadaException(evento.eventId(), new RuntimeException(evento.payload()))
        }
        1 * outbox.marcarReintento(_, _, _, _) >> { args -> guardado = args[3]; true }
        guardado == "com.factoring.commons.auditoria.AuditoriaNoPublicadaException <- java.lang.RuntimeException"
        !mensajes().contains(DATO_SENSIBLE)
    }

    def "lease vencido: vuelve a reintento contando un intento; al tope, FALLIDO"() {
        given:
        def vencido = reclamado("3", "A", intentosPrevios)

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(reloj.instant(), 50) >> [vencido]
        reintentos * outbox.marcarReintento(vencido, intentosPrevios + 1, _, ErrorSaneado.LEASE_VENCIDO) >> true
        fallidos * outbox.marcarFallido(vencido, intentosPrevios + 1, ErrorSaneado.LEASE_VENCIDO) >> true
        1 * outbox.tomarLote(_, _, _) >> []
        0 * publicador._

        where:
        intentosPrevios | reintentos | fallidos
        0               | 1          | 0
        2               | 0          | 1
    }

    def "orden por clave: tras el primer fallo de una clave se liberan sin publicar sus siguientes; las otras siguen"() {
        given:
        def a1 = reclamado("1", "A")
        def b1 = reclamado("2", "B")
        def a2 = reclamado("3", "A")
        def a3 = reclamado("4", "A")
        def b2 = reclamado("5", "B")
        def publicados = []

        when:
        def enviados = relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [a1, b1, a2, a3, b2]
        3 * publicador.publicarConfirmado(_, _, _) >> { args ->
            publicados << args[0].eventId()
            if (args[0].eventId() == a1.eventId()) {
                throw new AuditoriaNoPublicadaException(a1.eventId(), new RuntimeException())
            }
        }
        1 * outbox.marcarReintento(a1, 1, _, _) >> true
        1 * outbox.liberar(a2) >> true
        1 * outbox.liberar(a3) >> true
        1 * outbox.marcarEnviado("2", _)
        1 * outbox.marcarEnviado("5", _)
        publicados == [a1.eventId(), b1.eventId(), b2.eventId()]
        enviados == 2
    }

    def "payload ilegible: FALLIDO directo, sin publicar"() {
        given:
        def roto = new AuditoriaOutboxEvento("9", "ev-9", "FACTURA:Z", "auditoria.accion", "{no es json",
                EstadoOutbox.EN_PROCESO, 0, reloj.instant(), reloj.instant().plus(propiedades.lease),
                reloj.instant(), null, null)

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [roto]
        0 * publicador._
        1 * outbox.marcarFallido(roto, 1, ErrorSaneado.PAYLOAD_ILEGIBLE) >> true
        !mensajes().contains("{no es json")
    }

    def "sin lease suficiente para esperar un ack, libera el resto del lote sin publicarlo"() {
        given:
        def e1 = reclamado("1", "A")
        def e2 = reclamado("2", "B")
        def e3 = reclamado("3", "C")

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [e1, e2, e3]
        1 * publicador.publicarConfirmado({ it.eventId() == e1.eventId() }, _, _) >> {
            reloj.avanzar(Duration.ofMinutes(4).plusSeconds(51))
        }
        1 * outbox.marcarEnviado("1", _)
        1 * outbox.liberar(e2) >> true
        1 * outbox.liberar(e3) >> true
        0 * publicador._
    }

    def "un marcado que no aplica (otro relay tomó el lease) no se registra como reintento ni como fallido"() {
        given:
        def evento = reclamado("1", "A", intentosPrevios)

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> [evento]
        1 * publicador.publicarConfirmado(_, _, _) >> { throw new AuditoriaNoPublicadaException("x", null) }
        _ * outbox.marcarReintento(*_) >> false
        _ * outbox.marcarFallido(*_) >> false
        logs.list.isEmpty()

        where:
        intentosPrevios << [0, 2]
    }

    def "ejecutarCicloSeguro registra el fallo del almacén sin propagarlo ni filtrar el mensaje"() {
        when:
        relay.ejecutarCicloSeguro()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> { throw new IllegalStateException("jdbc:secreto") }
        noExceptionThrown()
        mensajes().contains("java.lang.IllegalStateException")
        !mensajes().contains("jdbc:secreto")
    }

    def "ejecutarCicloSeguro con un ciclo normal no registra nada"() {
        when:
        relay.ejecutarCicloSeguro()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(_, _, _) >> []
        logs.list.isEmpty()
    }

    def "el lease pedido es ahora + auditoria.outbox.lease"() {
        given:
        Instant ahora = reloj.instant()

        when:
        relay.ejecutarCiclo()

        then:
        1 * outbox.buscarLeasesVencidos(_, _) >> []
        1 * outbox.tomarLote(50, ahora, ahora.plus(Duration.ofMinutes(5))) >> []
    }
}
