package com.factoring.commons.auditoria;

/** Catálogo de canales por los que llega una acción auditada. */
public final class Canal {

    /** Request HTTP de un front o de un cliente de la API. */
    public static final String WEB = "WEB";
    /** Carga automática por SFTP. */
    public static final String SFTP = "SFTP";
    /** Integración con un proveedor de facturación electrónica (Carvajal, Siigo). */
    public static final String PROVEEDOR_FE = "PROVEEDOR_FE";
    /** Consumo de un evento Kafka de otro servicio. */
    public static final String KAFKA = "KAFKA";
    /** Proceso agendado del propio servicio. */
    public static final String SCHEDULER = "SCHEDULER";

    private Canal() {
        throw new IllegalStateException("Utility class");
    }
}
