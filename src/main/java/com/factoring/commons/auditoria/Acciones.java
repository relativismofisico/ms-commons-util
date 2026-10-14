package com.factoring.commons.auditoria;

/**
 * Catálogo de acciones auditadas. Constantes String (no enum): agregar una acción nueva no rompe a un consumidor que
 * todavía no la conoce. Nombre en pasado: la acción ya ocurrió y quedó guardada.
 */
public final class Acciones {

    /** Registro anónimo de una empresa nueva (estado inicial). */
    public static final String EMPRESA_REGISTRADA = "EMPRESA_REGISTRADA";
    /** Cualquier transición de {@code company_state} (aprobación, devolución, rechazo, expiración, pasos del registro). */
    public static final String EMPRESA_ESTADO_CAMBIADO = "EMPRESA_ESTADO_CAMBIADO";

    /** Primera evidencia de una relación pagador–proveedor por una factura. */
    public static final String RELACION_COMERCIAL_CREADA = "RELACION_COMERCIAL_CREADA";
    /** El pagador inscribió al proveedor (crea la relación o la marca como inscrita). */
    public static final String RELACION_COMERCIAL_INSCRITA = "RELACION_COMERCIAL_INSCRITA";

    /** Creación de una negociación. */
    public static final String NEGOCIACION_CREADA = "NEGOCIACION_CREADA";
    /** Un actor aprobó una factura dentro de una negociación. */
    public static final String NEGOCIACION_FACTURA_APROBADA = "NEGOCIACION_FACTURA_APROBADA";
    /** Un actor rechazó una factura dentro de una negociación. */
    public static final String NEGOCIACION_FACTURA_RECHAZADA = "NEGOCIACION_FACTURA_RECHAZADA";
    /** Cambio del estado global de una negociación. */
    public static final String NEGOCIACION_ESTADO_CAMBIADO = "NEGOCIACION_ESTADO_CAMBIADO";

    /** Registro de un endoso. */
    public static final String ENDOSO_REGISTRADO = "ENDOSO_REGISTRADO";
    /** Firma (aprobación o rechazo) de un endoso con OTP. */
    public static final String ENDOSO_FIRMADO = "ENDOSO_FIRMADO";

    /** Liquidación de una factura. */
    public static final String FACTURA_LIQUIDADA = "FACTURA_LIQUIDADA";
    /** Intento de liquidación que terminó en error. */
    public static final String LIQUIDACION_FALLIDA = "LIQUIDACION_FALLIDA";

    /** Carga de una factura nueva. */
    public static final String FACTURA_CARGADA = "FACTURA_CARGADA";
    /** Recarga de una factura existente (mismo CUFE). */
    public static final String FACTURA_RECARGADA = "FACTURA_RECARGADA";

    /** Generación de una instrucción de pago. */
    public static final String INSTRUCCION_PAGO_GENERADA = "INSTRUCCION_PAGO_GENERADA";

    /** Cambio de contraseña (con su origen en {@code detalle}). */
    public static final String CONTRASENA_CAMBIADA = "CONTRASENA_CAMBIADA";

    /** Carga de un documento de empresa. */
    public static final String DOCUMENTO_EMPRESA_CARGADO = "DOCUMENTO_EMPRESA_CARGADO";
    /** Aprobación de un documento de empresa. */
    public static final String DOCUMENTO_EMPRESA_APROBADO = "DOCUMENTO_EMPRESA_APROBADO";
    /** Rechazo de un documento de empresa. */
    public static final String DOCUMENTO_EMPRESA_RECHAZADO = "DOCUMENTO_EMPRESA_RECHAZADO";
    /** Carga de un documento firmado. */
    public static final String DOCUMENTO_FIRMADO_CARGADO = "DOCUMENTO_FIRMADO_CARGADO";
    /** Aprobación de un documento firmado. */
    public static final String DOCUMENTO_FIRMADO_APROBADO = "DOCUMENTO_FIRMADO_APROBADO";
    /** Rechazo de un documento firmado. */
    public static final String DOCUMENTO_FIRMADO_RECHAZADO = "DOCUMENTO_FIRMADO_RECHAZADO";
    /** Registro de una firma (de empresa o de usuario). */
    public static final String FIRMA_REGISTRADA = "FIRMA_REGISTRADA";

    /** Conciliación de la reserva retenida de una factura. */
    public static final String RESERVA_CONCILIADA = "RESERVA_CONCILIADA";

    private Acciones() {
        throw new IllegalStateException("Utility class");
    }
}
