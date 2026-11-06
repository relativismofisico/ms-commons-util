package com.factoring.commons.auditoria.outbox

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

/** Reloj que solo avanza cuando el test lo pide (en milisegundos, como los almacenes). */
class RelojDePrueba extends Clock {

    private volatile Instant actual

    RelojDePrueba(Instant inicio = Instant.now()) {
        actual = inicio.truncatedTo(ChronoUnit.MILLIS)
    }

    void avanzar(Duration duracion) {
        actual = actual.plus(duracion)
    }

    @Override
    ZoneId getZone() { ZoneOffset.UTC }

    @Override
    Clock withZone(ZoneId zone) { this }

    @Override
    Instant instant() { actual }
}
