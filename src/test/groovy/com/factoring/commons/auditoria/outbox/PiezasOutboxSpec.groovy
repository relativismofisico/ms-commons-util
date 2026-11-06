package com.factoring.commons.auditoria.outbox

import com.factoring.commons.auditoria.Acciones
import com.factoring.commons.auditoria.AuditoriaAccionEvent
import com.factoring.commons.auditoria.AuditoriaActor
import com.factoring.commons.auditoria.AuditoriaJson
import com.factoring.commons.auditoria.Canal
import com.factoring.commons.auditoria.EntidadTipo
import net.javacrumbs.shedlock.core.LockConfiguration
import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.core.SimpleLock
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Piezas chicas del outbox: orden por clave, error saneado, propiedades, puerto, limpieza, bloqueo y planificador. */
class PiezasOutboxSpec extends Specification {

    // ── OrdenPorClave ─────────────────────────────────────────────────────────

    def "orden por clave: un candidato se toma solo si ningún anterior no enviado de su clave queda fuera"() {
        given:
        def filas = [
                new OrdenPorClave.Fila("1", "A", true),
                new OrdenPorClave.Fila("2", "B", false),   // B bloqueada (p. ej. en reintento con backoff)
                new OrdenPorClave.Fila("3", "B", true),
                new OrdenPorClave.Fila("4", "A", true),
                new OrdenPorClave.Fila("5", "C", true),
                new OrdenPorClave.Fila("6", "C", false),
                new OrdenPorClave.Fila("7", "C", true)]

        expect:
        OrdenPorClave.elegibles(filas) as List == ["1", "4", "5"]
    }

    // ── ErrorSaneado ──────────────────────────────────────────────────────────

    def "error saneado: solo la cadena de tipos, con tope de profundidad, ciclos y largo"() {
        given:
        def raiz = new IllegalStateException("payload {secreto}")
        def ciclo = new RuntimeException("a")
        def otro = new IllegalArgumentException("b", ciclo)
        ciclo.initCause(otro)
        def profundo = (1..8).inject(new RuntimeException("0")) { Throwable c, n -> new RuntimeException("${n}", c) }

        expect:
        ErrorSaneado.de(new RuntimeException("x", raiz)) == "java.lang.RuntimeException <- java.lang.IllegalStateException"
        ErrorSaneado.de(ciclo) == "java.lang.RuntimeException <- java.lang.IllegalArgumentException"
        ErrorSaneado.de(profundo).count("<-") == 4
        ErrorSaneado.de(new ExcepcionDeNombreLargo()).length() <= ErrorSaneado.LARGO_MAXIMO
        !ErrorSaneado.de(raiz).contains("secreto")
    }

    // ── AuditoriaOutboxProperties ─────────────────────────────────────────────

    def "propiedades: por defecto apagado y valores documentados"() {
        given:
        def p = new AuditoriaOutboxProperties()

        expect:
        !p.enabled
        p.almacen == null
        p.tamanoLote == 50
        p.intervalo == Duration.ofSeconds(5)
        p.maxIntentos == 10
        p.backoffBase == Duration.ofSeconds(5)
        p.backoffMax == Duration.ofMinutes(10)
        p.retencion == Duration.ofDays(30)
        p.intervaloLimpieza == Duration.ofHours(1)
        p.lease == Duration.ofMinutes(5)
        p.timeoutAck == Duration.ofSeconds(30)
        p.mongo.coleccion == null
    }

    def "propiedades: validar acepta lo coherente"() {
        when:
        new AuditoriaOutboxProperties(almacen: AuditoriaOutboxProperties.Almacen.MONGO).validar()

        then:
        noExceptionThrown()
    }

    def "propiedades: validar rechaza lo incoherente nombrando la propiedad"() {
        given:
        def p = new AuditoriaOutboxProperties(almacen: AuditoriaOutboxProperties.Almacen.JDBC)
        cambio(p)

        when:
        p.validar()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains(nombre)

        where:
        nombre               | cambio
        "almacen"            | { it.almacen = null }
        "tamano-lote"        | { it.tamanoLote = 0 }
        "max-intentos"       | { it.maxIntentos = 0 }
        "intervalo "         | { it.intervalo = Duration.ZERO }
        "intervalo-limpieza" | { it.intervaloLimpieza = Duration.ofSeconds(-1) }
        "backoff-base"       | { it.backoffBase = null }
        "retencion"          | { it.retencion = Duration.ZERO }
        "timeout-ack"        | { it.timeoutAck = Duration.ZERO }
        "backoff-max"        | { it.backoffMax = Duration.ofSeconds(1) }
        "lease"              | { it.lease = Duration.ofSeconds(30) }
    }

    // ── OutboxAuditoriaPort ───────────────────────────────────────────────────

    def "el puerto guarda el evento serializado, con su eventId y clave, y completa servicioOrigen"() {
        given:
        AuditoriaOutboxPort outbox = Mock()
        def reloj = new RelojDePrueba()
        def puerto = new OutboxAuditoriaPort(outbox, "auditoria.accion", "ms-prueba", reloj)
        def accion = evento(servicio)
        AuditoriaOutboxEvento guardado = null

        when:
        puerto.registrar(accion)

        then:
        1 * outbox.guardar(_) >> { args -> guardado = args[0] }
        guardado.id() == null
        guardado.eventId() == accion.eventId()
        guardado.claveKafka() == "FACTURA:CUFE-1"
        guardado.topic() == "auditoria.accion"
        guardado.estado() == EstadoOutbox.PENDIENTE
        guardado.intentos() == 0
        guardado.proximoIntento() == reloj.instant()
        guardado.fechaCreacion() == reloj.instant()
        AuditoriaJson.deserializar(guardado.payload()).servicioOrigen() == esperado

        where:
        servicio | esperado
        null     | "ms-prueba"
        " "      | "ms-prueba"
        "ms-otro"| "ms-otro"
    }

    def "si la escritura del outbox falla, el puerto propaga la excepción"() {
        given:
        AuditoriaOutboxPort outbox = Mock() { guardar(_) >> { throw new IllegalStateException("bd caída") } }

        when:
        new OutboxAuditoriaPort(outbox, "t", "ms", new RelojDePrueba()).registrar(evento(null))

        then:
        thrown(IllegalStateException)
    }

    // ── AuditoriaOutboxLimpieza ───────────────────────────────────────────────

    def "limpieza: borra enviados más viejos que la retención, en tandas, bajo el bloqueo del servicio"() {
        given:
        AuditoriaOutboxPort outbox = Mock()
        def reloj = new RelojDePrueba()
        def nombres = []
        BloqueoLimpieza bloqueo = { String nombre, Runnable tarea -> nombres << nombre; tarea.run() } as BloqueoLimpieza
        def limpieza = new AuditoriaOutboxLimpieza(outbox, new AuditoriaOutboxProperties(), bloqueo, "ms-prueba", reloj)

        when:
        def borrados = limpieza.limpiar()

        then:
        2 * outbox.limpiarEnviadosAnteriores(reloj.instant().minus(Duration.ofDays(30)), AuditoriaOutboxLimpieza.TANDA) >>> [1000, 7]
        borrados == 1007
        nombres == ["auditoria-outbox-limpieza-ms-prueba"]
    }

    def "limpieza: si otra instancia tiene el bloqueo no borra nada"() {
        given:
        AuditoriaOutboxPort outbox = Mock()
        BloqueoLimpieza ocupado = { String n, Runnable t -> } as BloqueoLimpieza

        expect:
        new AuditoriaOutboxLimpieza(outbox, new AuditoriaOutboxProperties(), ocupado, "ms", new RelojDePrueba()).limpiar() == 0
    }

    def "limpiarSeguro no propaga ni filtra el mensaje del fallo"() {
        given:
        AuditoriaOutboxPort outbox = Mock()
        def limpieza = new AuditoriaOutboxLimpieza(outbox, new AuditoriaOutboxProperties(), BloqueoLimpieza.SIN_BLOQUEO,
                "ms", new RelojDePrueba())

        when:
        limpieza.limpiarSeguro()
        limpieza.limpiarSeguro()
        limpieza.limpiarSeguro()

        then:
        3 * outbox.limpiarEnviadosAnteriores(_, _) >>> [5, 0] >> { throw new IllegalStateException("x") }
        noExceptionThrown()
    }

    def "ShedLock: ejecuta la limpieza solo con el bloqueo, con lockAtMostFor = intervalo de limpieza"() {
        given:
        LockProvider proveedor = Mock()
        SimpleLock lock = Mock()
        def reloj = new RelojDePrueba()
        def bloqueo = new ShedLockBloqueoLimpieza(proveedor, Duration.ofHours(1), reloj)
        def corridas = 0

        when:
        bloqueo.ejecutar("auditoria-outbox-limpieza-ms", { corridas++ })
        bloqueo.ejecutar("auditoria-outbox-limpieza-ms", { corridas++ })

        then:
        2 * proveedor.lock({ LockConfiguration c ->
            c.name == "auditoria-outbox-limpieza-ms" && c.lockAtMostFor == Duration.ofHours(1)
        }) >>> [Optional.of(lock), Optional.empty()]
        1 * lock.unlock()
        corridas == 1
    }

    // ── AuditoriaOutboxPlanificador ───────────────────────────────────────────

    def "planificador: corre el relay y la limpieza en su hilo y se detiene limpio"() {
        given:
        def ciclos = new CountDownLatch(2)
        def limpiezas = new CountDownLatch(1)
        OutboxAuditoriaRelay relay = Mock() { ejecutarCicloSeguro() >> { ciclos.countDown() } }
        AuditoriaOutboxLimpieza limpieza = Mock() { limpiarSeguro() >> { limpiezas.countDown() } }
        def propiedades = new AuditoriaOutboxProperties(intervalo: Duration.ofMillis(20),
                intervaloLimpieza: Duration.ofMillis(30))
        def planificador = new AuditoriaOutboxPlanificador(relay, limpieza, propiedades)

        when:
        planificador.start()
        planificador.start()

        then:
        planificador.running
        ciclos.await(5, TimeUnit.SECONDS)
        limpiezas.await(5, TimeUnit.SECONDS)

        when:
        planificador.stop()
        planificador.stop()

        then:
        !planificador.running
    }

    def "planificador: si lo interrumpen mientras espera el cierre, corta y conserva la interrupción"() {
        given:
        def enCiclo = new CountDownLatch(1)
        OutboxAuditoriaRelay relay = Mock() { ejecutarCicloSeguro() >> { enCiclo.countDown(); sleep(5000) } }
        def planificador = new AuditoriaOutboxPlanificador(relay, Mock(AuditoriaOutboxLimpieza),
                new AuditoriaOutboxProperties(intervalo: Duration.ofMillis(10)))
        planificador.start()
        enCiclo.await(5, TimeUnit.SECONDS)

        when:
        Thread.currentThread().interrupt()
        planificador.stop()

        then:
        Thread.interrupted()
        !planificador.running
    }

    // ── AuditoriaOutboxEvento ─────────────────────────────────────────────────

    def "evento nuevo y en proceso"() {
        given:
        def ahora = new RelojDePrueba().instant()
        def nuevo = AuditoriaOutboxEvento.nuevo("ev", "A:1", "t", "{}", ahora)

        when:
        def reclamado = nuevo.enProceso(ahora.plusSeconds(60))

        then:
        reclamado.estado() == EstadoOutbox.EN_PROCESO
        reclamado.leaseHasta() == ahora.plusSeconds(60)
        reclamado.eventId() == "ev"
        reclamado.intentos() == 0
    }

    private static AuditoriaAccionEvent evento(String servicio) {
        AuditoriaAccionEvent.builder()
                .servicioOrigen(servicio)
                .accion(Acciones.FACTURA_CARGADA)
                .entidad(EntidadTipo.FACTURA, "CUFE-1")
                .actor(AuditoriaActor.usuario("u1", "EMPRESA", 1L))
                .canal(Canal.WEB)
                .build()
    }

    /** Excepción con un nombre de clase cualquiera, para el tope de largo. */
    static class ExcepcionDeNombreLargo extends RuntimeException {
        ExcepcionDeNombreLargo() {
            super("m", (1..10).inject(null) { Throwable c, n -> new ExcepcionDeNombreLargoAnidadaConMuchoTexto(c) })
        }
    }

    static class ExcepcionDeNombreLargoAnidadaConMuchoTexto extends RuntimeException {
        ExcepcionDeNombreLargoAnidadaConMuchoTexto(Throwable causa) {
            super("m", causa)
        }
    }
}
