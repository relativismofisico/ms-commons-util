-- Tabla del outbox con el esquema 0.6.0 (MySQL 8): la de 0.5.x más la columna tipo. SQL de referencia: el mismo que
-- muestra el README; cada servicio lo pone en su propia migración Flyway, con su nombre de tabla.
CREATE TABLE outbox_ms_prueba (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    event_id        VARCHAR(64)  NOT NULL,
    tipo            VARCHAR(100) NULL,
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
    UNIQUE KEY ux_outbox_event_id (event_id),
    KEY ix_outbox_lote (estado, proximo_intento, id),
    KEY ix_outbox_clave (clave_kafka, estado, id),
    KEY ix_outbox_limpieza (estado, fecha_envio)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
