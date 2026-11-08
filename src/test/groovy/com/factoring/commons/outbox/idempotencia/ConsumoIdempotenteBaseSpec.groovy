package com.factoring.commons.outbox.idempotencia

import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Consumo idempotente contra un almacén real; las subclases dan el almacén y un efecto en ese mismo almacén. */
abstract class ConsumoIdempotenteBaseSpec extends Specification {

    abstract ConsumoIdempotente consumo()

    /** Escribe un registro de negocio en el mismo almacén, dentro de la transacción en curso. */
    abstract void escribirNegocio(String id)

    abstract long negocios(String id)

    abstract long marcas()

    /** Deja una marca con una fecha de procesado arbitraria. */
    abstract void marcaAntigua(String consumidor, String clave, Instant fecha)

    abstract void vaciar()

    /** Un fallo transitorio de concurrencia del almacén, que el consumidor reintenta. */
    abstract boolean esTransitorio(Throwable error)

    def setup() {
        vaciar()
    }

    def "un evento repetido aplica el efecto una sola vez"() {
        when:
        def primero = consumo().procesar("consumidor.a", "evt-1", { escribirNegocio("N-1") })
        def segundo = consumo().procesar("consumidor.a", "evt-1", { escribirNegocio("N-1") })

        then:
        primero
        !segundo
        negocios("N-1") == 1
        marcas() == 1
    }

    def "la marca es por consumidor: otro consumidor del mismo evento aplica su efecto"() {
        when:
        consumo().procesar("consumidor.a", "evt-2", { escribirNegocio("N-2a") })
        def otro = consumo().procesar("consumidor.b", "evt-2", { escribirNegocio("N-2b") })

        then:
        otro
        negocios("N-2a") == 1
        negocios("N-2b") == 1
    }

    def "si el efecto falla se revierten la marca y el efecto, y el reintento lo aplica"() {
        when:
        consumo().procesar("consumidor.a", "evt-3", {
            escribirNegocio("N-3")
            throw new IllegalStateException("falla el efecto")
        })

        then:
        thrown(IllegalStateException)
        negocios("N-3") == 0
        marcas() == 0

        when:
        def reintento = consumo().procesar("consumidor.a", "evt-3", { escribirNegocio("N-3") })

        then:
        reintento
        negocios("N-3") == 1
    }

    def "sin eventId usa la clave natural con el prefijo nat:"() {
        when:
        def primero = consumo().procesar("consumidor.a", null, "usuario:7", { escribirNegocio("N-4") })
        def segundo = consumo().procesar("consumidor.a", " ", "usuario:7", { escribirNegocio("N-4") })

        then:
        primero
        !segundo
        negocios("N-4") == 1
        ConsumoIdempotente.clave(null, "usuario:7") == "nat:usuario:7"
        ConsumoIdempotente.clave("evt-9", "usuario:7") == "evt-9"
    }

    def "sin eventId ni clave natural, o con un consumidor vacío, se rechaza sin escribir"() {
        when:
        consumo().procesar(consumidor, eventId, natural, { escribirNegocio("N-5") })

        then:
        thrown(IllegalArgumentException)
        negocios("N-5") == 0

        where:
        consumidor     | eventId | natural
        "consumidor.a" | null    | null
        "consumidor.a" | ""      | " "
        " "            | "evt-5" | null
        "x" * 101      | "evt-5" | null
    }

    def "entregas concurrentes del mismo evento: el efecto se aplica una sola vez"() {
        given:
        int hilos = 8
        def listos = new CountDownLatch(hilos)
        def largada = new CountDownLatch(1)
        def aplicados = new AtomicInteger()
        def errores = new CopyOnWriteArrayList<Throwable>()

        when:
        def threads = (1..hilos).collect {
            Thread.start {
                listos.countDown()
                largada.await(10, TimeUnit.SECONDS)
                // Como el consumidor Kafka: un fallo transitorio se reintenta
                for (int intento = 0; intento < 5; intento++) {
                    try {
                        if (consumo().procesar("consumidor.a", "evt-6", { escribirNegocio("N-6"); Thread.sleep(200) })) {
                            aplicados.incrementAndGet()
                        }
                        return
                    } catch (Throwable t) {
                        if (!esTransitorio(t)) {
                            errores << t
                            return
                        }
                    }
                }
            }
        }
        listos.await(10, TimeUnit.SECONDS)
        largada.countDown()
        threads*.join(30_000)

        then:
        errores.empty
        aplicados.get() == 1
        negocios("N-6") == 1
        marcas() == 1
    }

    def "la limpieza borra solo las marcas más viejas que la retención"() {
        given:
        def ahora = Instant.now()
        marcaAntigua("consumidor.a", "vieja", ahora.minus(Duration.ofDays(31)))
        marcaAntigua("consumidor.a", "reciente", ahora.minus(Duration.ofDays(1)))
        def limpieza = new LimpiezaConsumoIdempotente(consumo(), Duration.ofDays(30), Duration.ofHours(1),
                java.time.Clock.systemUTC())

        when:
        def borradas = limpieza.limpiar()

        then:
        borradas == 1
        marcas() == 1
        !consumo().procesar("consumidor.a", "reciente", { escribirNegocio("N-7") })
        consumo().procesar("consumidor.a", "vieja", { escribirNegocio("N-7") })
    }
}
