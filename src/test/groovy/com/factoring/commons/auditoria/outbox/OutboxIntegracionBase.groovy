package com.factoring.commons.auditoria.outbox

import com.factoring.commons.auditoria.Acciones
import com.factoring.commons.auditoria.AuditoriaAccionEvent
import com.factoring.commons.auditoria.AuditoriaActor
import com.factoring.commons.auditoria.AuditoriaNoPublicadaException
import com.factoring.commons.auditoria.AuditoriaPublisher
import com.factoring.commons.auditoria.Canal
import com.factoring.commons.auditoria.EntidadTipo
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Pruebas de integración comunes a los dos almacenes del outbox, contra la base real (Testcontainers). Cada subclase
 * pone el almacén, sus transacciones y cómo leer lo guardado.
 */
abstract class OutboxIntegracionBase extends Specification {

    RelojDePrueba reloj = new RelojDePrueba()
    AuditoriaOutboxProperties propiedades = new AuditoriaOutboxProperties(
            almacen: AuditoriaOutboxProperties.Almacen.JDBC, tamanoLote: 10, maxIntentos: 5,
            backoffBase: Duration.ofSeconds(1), backoffMax: Duration.ofSeconds(10), lease: Duration.ofMinutes(1),
            timeoutAck: Duration.ofSeconds(2))

    abstract AuditoriaOutboxPort outbox()

    /** Ejecuta el trabajo en una transacción del almacén; con {@code revertir} la revierte al final. */
    abstract void enTransaccion(boolean revertir, Closure trabajo)

    /** Escribe un registro de negocio en el mismo almacén (dentro de la transacción en curso, si la hay). */
    abstract void escribirNegocio(String id)

    abstract boolean existeNegocio(String id)

    /** El evento guardado con ese eventId, o null. */
    abstract AuditoriaOutboxEvento leer(String eventId)

    abstract long noEnviados()

    abstract void vaciar()

    /** Fuerza el estado de un evento (lo que haría un operador o un relay a medias). */
    abstract void forzar(String eventId, EstadoOutbox estado, int intentos, Instant proximoIntento, Instant leaseHasta)

    def setup() {
        vaciar()
    }

    OutboxAuditoriaPort puerto(String topic = "auditoria.accion") {
        new OutboxAuditoriaPort(outbox(), topic, "ms-prueba", reloj)
    }

    OutboxAuditoriaRelay relay(AuditoriaPublisher publicador) {
        new OutboxAuditoriaRelay(outbox(), publicador, propiedades, reloj)
    }

    static AuditoriaAccionEvent evento(String entidadId) {
        AuditoriaAccionEvent.builder()
                .accion(Acciones.FACTURA_CARGADA)
                .entidad(EntidadTipo.FACTURA, entidadId)
                .actor(AuditoriaActor.usuario("u1", "EMPRESA", 1L))
                .canal(Canal.WEB)
                .detalle("observacion", "dato")
                .build()
    }

    /** Publicador que registra lo que "Kafka confirmó", en orden. */
    static class Grabador implements AuditoriaPublisher {
        final List<AuditoriaAccionEvent> publicados = new CopyOnWriteArrayList<>()

        void publicar(AuditoriaAccionEvent e) {
            throw new UnsupportedOperationException()
        }

        void publicarConfirmado(AuditoriaAccionEvent e) {
            Thread.sleep(1)
            publicados << e
        }

        List<String> ids() { publicados*.eventId() }
    }

    def "escritura dentro de una transacción confirmada: el evento queda PENDIENTE junto con el cambio de negocio"() {
        given:
        def e = evento("T-1")

        when:
        enTransaccion(false) {
            escribirNegocio("negocio-1")
            puerto().registrar(e)
        }

        then:
        existeNegocio("negocio-1")
        def guardado = leer(e.eventId())
        guardado.estado() == EstadoOutbox.PENDIENTE
        guardado.claveKafka() == "FACTURA:T-1"
        guardado.topic() == "auditoria.accion"
        guardado.intentos() == 0
        guardado.payload().contains(e.eventId())
    }

    def "rollback de la transacción: no queda ni el cambio de negocio ni el evento"() {
        given:
        def e = evento("T-2")

        when:
        enTransaccion(true) {
            escribirNegocio("negocio-2")
            puerto().registrar(e)
        }

        then:
        !existeNegocio("negocio-2")
        leer(e.eventId()) == null
    }

    def "sin transacción activa el evento se escribe igual y queda duradero"() {
        given:
        def e = evento("T-3")

        when:
        puerto().registrar(e)

        then:
        leer(e.eventId()).estado() == EstadoOutbox.PENDIENTE
    }

    def "un eventId repetido no se guarda dos veces: la excepción llega al caso de uso"() {
        given:
        def e = evento("T-4")
        puerto().registrar(e)

        when:
        puerto().registrar(e)

        then:
        thrown(RuntimeException)
    }

    def "dos relays concurrentes no publican dos veces el mismo evento y respetan el orden por clave"() {
        given:
        def eventos = (1..120).collect { evento("C-" + (it % 15)) }
        eventos.each { puerto().registrar(it) }
        def grabador = new Grabador()
        def relays = [relay(grabador), relay(grabador)]
        def pool = Executors.newFixedThreadPool(2)
        def listo = new CountDownLatch(1)
        def errores = new CopyOnWriteArrayList<Throwable>()

        when:
        relays.each { r ->
            pool.submit {
                listo.await()
                long hasta = System.currentTimeMillis() + 60_000
                try {
                    while (noEnviados() > 0 && System.currentTimeMillis() < hasta) {
                        r.ejecutarCiclo()
                    }
                } catch (Throwable t) {
                    errores << t
                }
            }
        }
        listo.countDown()
        pool.shutdown()
        pool.awaitTermination(90, TimeUnit.SECONDS)

        then:
        errores.isEmpty()
        noEnviados() == 0
        grabador.ids().size() == 120
        grabador.ids() as Set == eventos*.eventId() as Set
        eventos.groupBy { it.claveKafka() }.every { clave, propios ->
            grabador.publicados.findAll { it.claveKafka() == clave }*.eventId() == propios*.eventId()
        }
    }

    def "caída entre reclamar y publicar: al vencer el lease vuelve a reintento y sale una vez, en orden"() {
        given:
        def a1 = evento("K-A")
        def a2 = evento("K-A")
        def b1 = evento("K-B")
        [a1, a2, b1].each { puerto().registrar(it) }
        def grabador = new Grabador()

        when: "un relay reclama el lote y cae sin publicar ni marcar"
        def reclamados = outbox().tomarLote(10, reloj.instant(), reloj.instant().plus(propiedades.lease))

        then:
        reclamados*.eventId() == [a1, a2, b1]*.eventId()
        reclamados.every { it.estado() == EstadoOutbox.EN_PROCESO }

        when: "otro relay corre mientras el lease sigue vigente"
        def enviados = relay(grabador).ejecutarCiclo()

        then:
        enviados == 0
        grabador.publicados.isEmpty()

        when: "vence el lease: vuelven a reintento con backoff"
        reloj.avanzar(propiedades.lease.plusSeconds(1))
        relay(grabador).ejecutarCiclo()

        then:
        grabador.publicados.isEmpty()
        [a1, a2, b1].every {
            def g = leer(it.eventId())
            g.estado() == EstadoOutbox.PENDIENTE && g.intentos() == 1 && g.ultimoError() == ErrorSaneado.LEASE_VENCIDO
        }

        when: "pasado el backoff"
        reloj.avanzar(Duration.ofSeconds(2))
        relay(grabador).ejecutarCiclo()

        then:
        grabador.ids() == [a1, a2, b1]*.eventId()
        [a1, a2, b1].every { leer(it.eventId()).estado() == EstadoOutbox.ENVIADO }
    }

    def "un evento en backoff bloquea a los siguientes de su clave, no a los de otras claves"() {
        given:
        def a1 = evento("O-A")
        def a2 = evento("O-A")
        def b1 = evento("O-B")
        [a1, a2, b1].each { puerto().registrar(it) }
        def fallaA1 = new Grabador() {
            @Override
            void publicarConfirmado(AuditoriaAccionEvent e) {
                if (e.eventId() == a1.eventId()) {
                    throw new AuditoriaNoPublicadaException(e.eventId(), new RuntimeException())
                }
                super.publicarConfirmado(e)
            }
        }

        when:
        relay(fallaA1).ejecutarCiclo()
        relay(fallaA1).ejecutarCiclo()

        then:
        fallaA1.ids() == [b1.eventId()]
        leer(a1.eventId()).estado() == EstadoOutbox.PENDIENTE
        leer(a1.eventId()).intentos() == 1
        leer(a2.eventId()).estado() == EstadoOutbox.PENDIENTE
        leer(a2.eventId()).intentos() == 0

        when:
        def grabador = new Grabador()
        reloj.avanzar(Duration.ofSeconds(2))
        relay(grabador).ejecutarCiclo()

        then:
        grabador.ids() == [a1, a2]*.eventId()
    }

    def "agotados los intentos queda FALLIDO y bloquea su clave"() {
        given:
        propiedades.maxIntentos = 2
        def a1 = evento("F-A")
        def a2 = evento("F-A")
        [a1, a2].each { puerto().registrar(it) }
        AuditoriaPublisher siempreFalla = Stub() {
            publicarConfirmado(_, _, _) >> { throw new AuditoriaNoPublicadaException("x", new RuntimeException()) }
        }

        when:
        relay(siempreFalla).ejecutarCiclo()
        reloj.avanzar(Duration.ofSeconds(2))
        relay(siempreFalla).ejecutarCiclo()
        reloj.avanzar(Duration.ofSeconds(20))
        def grabador = new Grabador()
        relay(grabador).ejecutarCiclo()

        then:
        leer(a1.eventId()).estado() == EstadoOutbox.FALLIDO
        leer(a1.eventId()).intentos() == 2
        grabador.publicados.isEmpty()
        leer(a2.eventId()).estado() == EstadoOutbox.PENDIENTE
    }

    def "Kafka caído: el evento queda PENDIENTE con su error y sale cuando Kafka vuelve"() {
        given:
        def topic = "auditoria.it." + UUID.randomUUID()
        def publicador = KafkaDePrueba.publicador(topic)
        def previo = evento("K-0")
        puerto(topic).registrar(previo)
        assert relay(publicador).ejecutarCiclo() == 1
        def e = evento("K-1")

        when:
        KafkaDePrueba.pausar()
        puerto(topic).registrar(e)
        def enviadosConKafkaCaido = relay(publicador).ejecutarCiclo()
        def mientrasCaido = leer(e.eventId())

        then:
        enviadosConKafkaCaido == 0
        mientrasCaido.estado() == EstadoOutbox.PENDIENTE
        mientrasCaido.intentos() == 1
        mientrasCaido.ultimoError().startsWith("com.factoring.commons.auditoria.AuditoriaNoPublicadaException")
        !mientrasCaido.ultimoError().contains("dato")

        when:
        KafkaDePrueba.reanudar()
        int ciclos = 0
        while (leer(e.eventId()).estado() != EstadoOutbox.ENVIADO && ciclos++ < 10) {
            reloj.avanzar(Duration.ofSeconds(11))
            relay(publicador).ejecutarCiclo()
        }

        then:
        leer(e.eventId()).estado() == EstadoOutbox.ENVIADO
        KafkaDePrueba.eventIdsPublicados(topic, 2).containsAll([previo.eventId(), e.eventId()])

        cleanup:
        try {
            KafkaDePrueba.reanudar()
        } catch (ignored) {
            // ya estaba reanudado
        }
        publicador?.destroy()
    }

    def "la limpieza borra solo los ENVIADO más viejos que la retención"() {
        given:
        def viejo = evento("L-1")
        def reciente = evento("L-2")
        def pendiente = evento("L-3")
        def grabador = new Grabador()
        puerto().registrar(viejo)
        relay(grabador).ejecutarCiclo()
        reloj.avanzar(Duration.ofDays(31))
        puerto().registrar(reciente)
        relay(grabador).ejecutarCiclo()
        puerto().registrar(pendiente)

        when:
        def borrados = new AuditoriaOutboxLimpieza(outbox(), propiedades, BloqueoLimpieza.SIN_BLOQUEO, "ms-prueba",
                reloj).limpiar()

        then:
        borrados == 1
        leer(viejo.eventId()) == null
        leer(reciente.eventId()).estado() == EstadoOutbox.ENVIADO
        leer(pendiente.eventId()).estado() == EstadoOutbox.PENDIENTE
    }

    def "marcarEnviado sobre uno ya enviado no cambia nada; los marcados con lease ajeno no aplican"() {
        given:
        def e = evento("M-1")
        puerto().registrar(e)
        def reclamado = outbox().tomarLote(10, reloj.instant(), reloj.instant().plus(propiedades.lease)).first()
        def ajeno = reclamado.enProceso(reclamado.leaseHasta().plusSeconds(1))

        expect:
        !outbox().marcarReintento(ajeno, 1, reloj.instant(), "x")
        !outbox().marcarFallido(ajeno, 1, "x")
        !outbox().liberar(ajeno)
        outbox().liberar(reclamado)
        leer(e.eventId()).estado() == EstadoOutbox.PENDIENTE
        outbox().marcarEnviado(reclamado.id(), reloj.instant())
        !outbox().marcarEnviado(reclamado.id(), reloj.instant())
        outbox().buscarLeasesVencidos(reloj.instant().plus(Duration.ofDays(1)), 10).isEmpty()
        outbox().limpiarEnviadosAnteriores(reloj.instant().minus(Duration.ofDays(1)), 10) == 0
    }

    // ── Marcado tras el ack con reintentos locales ───────────────────────────

    /** Almacén real cuyo marcarEnviado falla las primeras {@code fallos} veces. */
    static class MarcadoQueFalla implements AuditoriaOutboxPort {
        @Delegate
        final AuditoriaOutboxPort real
        int fallos

        MarcadoQueFalla(AuditoriaOutboxPort real, int fallos) {
            this.real = real
            this.fallos = fallos
        }

        @Override
        boolean marcarEnviado(String id, Instant fechaEnvio) {
            if (fallos-- > 0) {
                throw new IllegalStateException("bd caída al marcar")
            }
            real.marcarEnviado(id, fechaEnvio)
        }
    }

    def "marcarEnviado falla dos veces y luego funciona: queda ENVIADO sin duplicar"() {
        given:
        def e = evento("MR-1")
        puerto().registrar(e)
        def grabador = new Grabador()
        def relay = new OutboxAuditoriaRelay(new MarcadoQueFalla(outbox(), 2), grabador, propiedades, reloj, {})

        when:
        relay.ejecutarCiclo()
        reloj.avanzar(propiedades.lease.plusSeconds(1))
        relay.ejecutarCiclo()

        then:
        leer(e.eventId()).estado() == EstadoOutbox.ENVIADO
        grabador.ids() == [e.eventId()]
    }

    def "marcarEnviado falla siempre: vuelve a reintento al vencer el lease y sale duplicado"() {
        given:
        def e = evento("MR-2")
        puerto().registrar(e)
        def grabador = new Grabador()
        def roto = new MarcadoQueFalla(outbox(), Integer.MAX_VALUE)

        when:
        new OutboxAuditoriaRelay(roto, grabador, propiedades, reloj, {}).ejecutarCiclo()

        then:
        leer(e.eventId()).estado() == EstadoOutbox.EN_PROCESO

        when: "vence el lease, pasa el backoff y el almacén vuelve"
        reloj.avanzar(propiedades.lease.plusSeconds(1))
        relay(grabador).ejecutarCiclo()
        def despuesDelLease = leer(e.eventId())
        reloj.avanzar(Duration.ofSeconds(2))
        relay(grabador).ejecutarCiclo()

        then:
        despuesDelLease.estado() == EstadoOutbox.PENDIENTE
        despuesDelLease.ultimoError() == ErrorSaneado.LEASE_VENCIDO
        leer(e.eventId()).estado() == EstadoOutbox.ENVIADO
        grabador.ids() == [e.eventId(), e.eventId()]
    }

    def "resumen: pendientes (PENDIENTE y EN_PROCESO), fallidos y el pendiente más antiguo"() {
        expect:
        outbox().resumen() == ResumenOutbox.VACIO

        when:
        def viejo = evento("RS-1")
        puerto().registrar(viejo)
        reloj.avanzar(Duration.ofSeconds(10))
        def fallido = evento("RS-2")
        puerto().registrar(fallido)
        puerto().registrar(evento("RS-3"))
        forzar(fallido.eventId(), EstadoOutbox.FALLIDO, 3, reloj.instant(), null)
        outbox().tomarLote(1, reloj.instant(), reloj.instant().plus(propiedades.lease))

        then:
        outbox().resumen() == new ResumenOutbox(2, 1, reloj.instant().minusSeconds(10))
    }

    // ── Inanición: claves bloqueadas delante no pueden frenar a las demás ─────

    /** tamanoLote + 10 eventos de la clave A, el primero de ellos devuelto para bloquearlo, y uno de la clave B detrás. */
    private Map registrarBloqueoDelante(String prefijo) {
        def a = (1..propiedades.tamanoLote + 11).collect { evento(prefijo + "-A") }
        a.each { puerto().registrar(it) }
        def b = evento(prefijo + "-B")
        puerto().registrar(b)
        [a: a, b: b]
    }

    def "inanición (a): un FALLIDO de A con tamanoLote+10 sucesores no impide publicar B en el primer ciclo"() {
        given:
        def eventos = registrarBloqueoDelante("IA")
        forzar(eventos.a[0].eventId(), EstadoOutbox.FALLIDO, propiedades.maxIntentos, reloj.instant(), null)
        def grabador = new Grabador()

        when:
        relay(grabador).ejecutarCiclo()

        then:
        grabador.ids() == [eventos.b.eventId()]
        eventos.a.drop(1).every { leer(it.eventId()).estado() == EstadoOutbox.PENDIENTE }
    }

    def "inanición (b): un evento de A en backoff con tamanoLote+10 sucesores no frena a B; los de A no salen"() {
        given:
        def eventos = registrarBloqueoDelante("IB")
        forzar(eventos.a[0].eventId(), EstadoOutbox.PENDIENTE, 1, reloj.instant().plusSeconds(60), null)
        def grabador = new Grabador()

        when:
        relay(grabador).ejecutarCiclo()

        then:
        grabador.ids() == [eventos.b.eventId()]
        eventos.a.every { leer(it.eventId()).estado() == EstadoOutbox.PENDIENTE }
    }

    def "inanición (c): un EN_PROCESO de A con lease vigente bloquea a sus sucesores, no a B"() {
        given:
        def eventos = registrarBloqueoDelante("IC")
        forzar(eventos.a[0].eventId(), EstadoOutbox.EN_PROCESO, 0, reloj.instant(), reloj.instant().plus(propiedades.lease))
        def grabador = new Grabador()

        when:
        relay(grabador).ejecutarCiclo()

        then:
        grabador.ids() == [eventos.b.eventId()]
        leer(eventos.a[0].eventId()).estado() == EstadoOutbox.EN_PROCESO
        eventos.a.drop(1).every { leer(it.eventId()).estado() == EstadoOutbox.PENDIENTE }
    }

    def "inanición (d): resuelto el FALLIDO (reintento manual), A sale completa y en orden"() {
        given:
        def eventos = registrarBloqueoDelante("ID")
        forzar(eventos.a[0].eventId(), EstadoOutbox.FALLIDO, propiedades.maxIntentos, reloj.instant(), null)
        def grabador = new Grabador()
        relay(grabador).ejecutarCiclo()

        when: "el operador lo devuelve a PENDIENTE con intentos = 0"
        forzar(eventos.a[0].eventId(), EstadoOutbox.PENDIENTE, 0, reloj.instant(), null)
        int ciclos = 0
        while (noEnviados() > 0 && ciclos++ < 10) {
            relay(grabador).ejecutarCiclo()
        }

        then:
        noEnviados() == 0
        grabador.ids() == [eventos.b.eventId()] + eventos.a*.eventId()
    }
}
