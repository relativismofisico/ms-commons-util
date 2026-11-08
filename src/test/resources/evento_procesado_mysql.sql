-- Marcas del consumo idempotente (MySQL 8). SQL de referencia: el mismo que muestra el README; cada servicio lo pone
-- en su propia migración Flyway.
CREATE TABLE IF NOT EXISTS evento_procesado (
    consumidor      VARCHAR(100) NOT NULL,
    clave           VARCHAR(191) NOT NULL,
    fecha_procesado DATETIME(6)  NOT NULL,
    PRIMARY KEY (consumidor, clave),
    KEY ix_evento_procesado_fecha (fecha_procesado)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
