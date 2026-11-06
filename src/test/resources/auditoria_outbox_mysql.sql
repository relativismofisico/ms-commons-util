-- Tabla del outbox de auditoría (MySQL 8). SQL de referencia: el mismo que muestra el README; cada servicio lo pone
-- en su propia migración Flyway.
CREATE TABLE auditoria_outbox (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    event_id        VARCHAR(64)  NOT NULL,
    clave_kafka     VARCHAR(255) NOT NULL,
    topic           VARCHAR(255) NOT NULL,
    payload         MEDIUMTEXT   NOT NULL,
    estado          VARCHAR(16)  NOT NULL,
    intentos        INT          NOT NULL DEFAULT 0,
    proximo_intento DATETIME(3)  NOT NULL,
    lease_hasta     DATETIME(3)  NULL,
    fecha_creacion  DATETIME(3)  NOT NULL,
    fecha_envio     DATETIME(3)  NULL,
    ultimo_error    VARCHAR(500) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY ux_auditoria_outbox_event_id (event_id),
    KEY ix_auditoria_outbox_lote (estado, proximo_intento, id),
    KEY ix_auditoria_outbox_clave (clave_kafka, estado, id),
    KEY ix_auditoria_outbox_limpieza (estado, fecha_envio)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
