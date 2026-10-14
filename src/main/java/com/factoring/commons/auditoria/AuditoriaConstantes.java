package com.factoring.commons.auditoria;

/** Nombres compartidos entre los productores de auditoría y ms-auditoria-util. */
public final class AuditoriaConstantes {

    /** Topic del evento ({@code auditoria.topic} lo puede cambiar por ambiente). */
    public static final String TOPIC_POR_DEFECTO = "auditoria.accion";

    /** Dead letter topic: eventos que el consumidor no pudo leer o validar. */
    public static final String TOPIC_DLT_POR_DEFECTO = TOPIC_POR_DEFECTO + ".DLT";

    /**
     * Único paquete que el consumidor acepta deserializar ({@code spring.json.trusted.packages}): nunca {@code "*"},
     * que deja al productor elegir qué clase instanciar del lado del consumidor.
     */
    public static final String PAQUETE_CONFIABLE = "com.factoring.commons.auditoria";

    private AuditoriaConstantes() {
        throw new IllegalStateException("Utility class");
    }
}
