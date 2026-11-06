package com.factoring.commons.auditoria.outbox;

/** Estado de un evento en el outbox de auditoría. */
public enum EstadoOutbox {

    /** Guardado y esperando su turno ({@code proximoIntento}) para publicarse. */
    PENDIENTE,

    /** Reclamado por un relay hasta {@code leaseHasta}; si el lease vence sin confirmarse, vuelve a reintento. */
    EN_PROCESO,

    /** Kafka confirmó la publicación. */
    ENVIADO,

    /** Agotó los intentos (o su payload es ilegible): queda para revisión manual y bloquea su clave Kafka. */
    FALLIDO
}
