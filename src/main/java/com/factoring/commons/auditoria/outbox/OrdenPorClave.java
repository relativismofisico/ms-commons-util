package com.factoring.commons.auditoria.outbox;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Regla de orden por clave Kafka que comparten los almacenes: un candidato solo se toma si ningún evento anterior no
 * enviado de su clave queda fuera del lote.
 */
public final class OrdenPorClave {

    private OrdenPorClave() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Fila no enviada de una de las claves del lote.
     *
     * @param id        id del evento
     * @param clave     clave Kafka
     * @param candidata si es uno de los candidatos a reclamar
     */
    public record Fila(String id, String clave, boolean candidata) {
    }

    /**
     * Candidatos que se pueden tomar.
     *
     * @param filas todas las filas no enviadas de las claves de los candidatos hasta el último candidato, en el orden
     *              del almacén
     * @return ids elegibles, en orden
     */
    public static Set<String> elegibles(final List<Fila> filas) {
        final Set<String> bloqueadas = new HashSet<>();
        final Set<String> elegidos = new LinkedHashSet<>();
        for (final Fila fila : filas) {
            if (!fila.candidata()) {
                bloqueadas.add(fila.clave());
            } else if (!bloqueadas.contains(fila.clave())) {
                elegidos.add(fila.id());
            }
        }
        return elegidos;
    }
}
