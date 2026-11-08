package com.factoring.commons.auditoria.outbox;

import java.util.HashSet;
import java.util.Set;

/**
 * Causa de un fallo apta para guardarse en {@code ultimoError} y para el log: solo la cadena de tipos de excepción,
 * nunca sus mensajes (un mensaje de Kafka o del driver puede llevar el registro, el payload o datos de conexión).
 */
public final class ErrorSaneado {

    /** Código fijo de un lease que venció sin que el relay confirmara ni marcara el evento. */
    public static final String LEASE_VENCIDO = "LEASE_VENCIDO";

    /** Código fijo de un payload que ya no se puede leer como evento de auditoría. */
    public static final String PAYLOAD_ILEGIBLE = "PAYLOAD_ILEGIBLE";

    /** Código fijo de un evento genérico sin publicador configurado (el servicio no tiene Kafka). */
    public static final String SIN_PUBLICADOR = "SIN_PUBLICADOR";

    /** Largo máximo (la columna {@code ultimo_error} es VARCHAR(500)). */
    static final int LARGO_MAXIMO = 500;

    private static final int PROFUNDIDAD_MAXIMA = 5;

    private ErrorSaneado() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * La cadena de causas como {@code Tipo <- CausaTipo <- ...}, sin mensajes.
     *
     * @param error el fallo
     * @return descripción saneada, de hasta {@value #LARGO_MAXIMO} caracteres
     */
    public static String de(final Throwable error) {
        final StringBuilder texto = new StringBuilder();
        final Set<Throwable> vistos = new HashSet<>();
        Throwable actual = error;
        int profundidad = 0;
        while (actual != null && profundidad < PROFUNDIDAD_MAXIMA && vistos.add(actual)) {
            if (profundidad > 0) {
                texto.append(" <- ");
            }
            texto.append(actual.getClass().getName());
            actual = actual.getCause();
            profundidad++;
        }
        if (texto.length() > LARGO_MAXIMO) {
            texto.setLength(LARGO_MAXIMO);
        }
        return texto.toString();
    }
}
