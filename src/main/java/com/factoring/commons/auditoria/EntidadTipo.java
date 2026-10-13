package com.factoring.commons.auditoria;

/** Catálogo de tipos de entidad auditada (String, por la misma razón que {@link Acciones}). */
public final class EntidadTipo {

    /** {@code company} (id = {@code ide_company}). */
    public static final String EMPRESA = "EMPRESA";
    /** {@code relacion_comercial} (id = {@code <pagador_nit>|<proveedor_nit>}). */
    public static final String RELACION_COMERCIAL = "RELACION_COMERCIAL";
    /** Negociación (id = id de la negociación). */
    public static final String NEGOCIACION = "NEGOCIACION";
    /** Endoso (id = id del endoso). */
    public static final String ENDOSO = "ENDOSO";
    /** Factura (id = CUFE si existe; si no, número de factura). */
    public static final String FACTURA = "FACTURA";
    /** Instrucción de pago (id = id del documento). */
    public static final String INSTRUCCION_PAGO = "INSTRUCCION_PAGO";
    /** Usuario (id = {@code ide_user}). */
    public static final String USUARIO = "USUARIO";
    /** Documento de empresa (id = id del archivo). */
    public static final String DOCUMENTO_EMPRESA = "DOCUMENTO_EMPRESA";
    /** Documento firmado (id = id del archivo). */
    public static final String DOCUMENTO_FIRMADO = "DOCUMENTO_FIRMADO";
    /** Firma registrada (id = id de la firma). */
    public static final String FIRMA = "FIRMA";
    /** Reserva retenida de una factura (id = número de factura). */
    public static final String RESERVA = "RESERVA";

    private EntidadTipo() {
        throw new IllegalStateException("Utility class");
    }
}
