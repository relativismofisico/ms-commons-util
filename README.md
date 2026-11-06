# ms-commons-util

Librería compartida de los backends de la plataforma Integrity (Factoring / Confirming). Se publica en GitHub
Packages como `com.factoring:ms-commons-util` al empujar un tag `v<versión>` (`publish.yml` comprueba que el tag
coincide con `version` de `build.gradle`).

| Paquete | Qué trae |
|---|---|
| `com.factoring.commons.util` | `IpUtils`: IP real del cliente detrás de proxy/LB |
| `com.factoring.commons.web` | `RateLimitFilter`: rate limit por IP respaldado por Redis |
| `com.factoring.commons.mdc` | Trazabilidad: `requestId` y actor en el MDC, propagación a Feign / RestTemplate / WebClient |
| `com.factoring.commons.auditoria` | Contrato `AuditoriaAccionEvent` y publicador Kafka autoconfigurado (`auditoria.*`) |
| `com.factoring.commons.auditoria.outbox` | Outbox transaccional de auditoría, opcional (`auditoria.outbox.*`, desde 0.5.0) |

Todas las dependencias de Spring, Kafka, JDBC, Mongo y ShedLock son `compileOnly`: cada pieza se registra solo si el
servicio trae la librería correspondiente.

La definición de terminado de la plataforma está en [DEFINITION_OF_DONE.md](DEFINITION_OF_DONE.md).

---

## Outbox de auditoría (0.5.0)

### Qué resuelve

Sin outbox, el adaptador de auditoría de cada servicio publica el evento en Kafka en `afterCommit`: si Kafka falla o el
proceso cae entre el commit y el envío, el evento solo queda en el logger `AUDITORIA_NO_PUBLICADO`.

Con el outbox, el evento se guarda **en el mismo almacén y en la misma transacción** que el cambio de negocio. Un relay
periódico lo publica después con ack de Kafka y recién entonces lo marca como enviado. Un rollback no deja evento, y
un commit no puede quedar sin su evento.

### Qué no cambia

- Por defecto **`auditoria.outbox.enabled=false`**. Un servicio que sube a 0.5.0 sin configurar nada no registra ningún
  bean nuevo y sigue exactamente como en 0.4.2 (`afterCommit` + `AUDITORIA_NO_PUBLICADO`).
- El contrato `AuditoriaAccionEvent`, el topic y el consumidor (ms-auditoria-util) no cambian. El consumidor ya
  descarta duplicados por `eventId`.
- `AuditoriaPublisher` gana un método `default` (`publicarConfirmado(evento, topic, espera)`): no rompe a quien lo
  implemente.

### Piezas

| Pieza | Qué hace |
|---|---|
| `AuditoriaOutboxEvento` | La fila: `id`, `eventId`, `claveKafka`, `topic`, `payload`, `estado` (`PENDIENTE`, `EN_PROCESO`, `ENVIADO`, `FALLIDO`), `intentos`, `proximoIntento`, `leaseHasta`, `fechaCreacion`, `fechaEnvio`, `ultimoError` |
| `AuditoriaOutboxPort` | Almacén: guardar en la transacción actual, tomar lote con lease, marcar enviado / reintento / fallido, liberar, buscar leases vencidos, limpiar enviados antiguos |
| `JdbcAuditoriaOutboxAdapter` | Tabla `auditoria_outbox` en el `DataSource` del servicio (MySQL 8) |
| `MongoAuditoriaOutboxAdapter` | Colección propia del servicio en su MongoDB |
| `com.factoring.commons.auditoria.AuditoriaPort` / `OutboxAuditoriaPort` | `registrar(evento)`: escribe en el outbox; si falla, la excepción se propaga |
| `OutboxAuditoriaRelay` | Reclama un lote, publica con ack, marca, reintenta con backoff |
| `AuditoriaOutboxLimpieza` | Borra los `ENVIADO` más viejos que la retención |
| `AuditoriaOutboxPlanificador` | Corre relay y limpieza en un hilo propio (no hace falta `@EnableScheduling`) |

### Cómo funciona el relay

En cada ciclo (`auditoria.outbox.intervalo`):

1. **Leases vencidos.** Un evento `EN_PROCESO` con `leaseHasta` vencido es uno que un relay reclamó y no llegó a marcar
   (cayó a medias). Vuelve a `PENDIENTE` contando un intento, con backoff, y `ultimoError = LEASE_VENCIDO`; si agotó
   los intentos pasa a `FALLIDO`.
2. **Reclamo del lote**, en orden de creación, con lease `ahora + auditoria.outbox.lease`:
   - JDBC: transacción propia y corta en READ COMMITTED con `SELECT ... FOR UPDATE SKIP LOCKED`; la publicación ocurre
     **fuera** de esa transacción.
   - Mongo: `findAndModify` documento a documento, condicionado a `estado = PENDIENTE`.
   - Dos relays nunca reciben el mismo evento.
3. **Orden por clave Kafka.** Un evento solo se toma si todos los anteriores no enviados de su misma `claveKafka`
   (`<entidad.tipo>:<entidad.id>`) también están en el lote. Ante el primer fallo de una clave, los siguientes de esa
   clave en el lote se liberan sin publicar.
4. **Publicación** con `publicarConfirmado` y espera acotada (`auditoria.outbox.timeout-ack`). Solo tras el ack se marca
   `ENVIADO`. Si falla: `intentos + 1` y `proximoIntento = ahora + backoff`, con
   `backoff = backoff-base * 2^(intentos-1)` y tope `backoff-max`. Al llegar a `max-intentos` pasa a `FALLIDO` con un log
   `ERROR` `[AUDITORIA][OUTBOX_FALLIDO]` que lleva id, `eventId`, clave, intentos y causa, **nunca el payload**.
5. Si al relay ya no le queda lease para esperar un ack completo, libera el resto del lote sin publicarlo.

Los marcados exigen el mismo lease que se reclamó: un relay con el lease vencido no pisa lo que hizo otro. Si una
instancia cae entre el ack y el marcado, el evento sale dos veces con el mismo `eventId` y el consumidor descarta el
duplicado. `ultimoError` solo guarda la cadena de tipos de excepción (o un código fijo), nunca mensajes ni payload.

**Bloqueo distribuido:** el relay **no** lo necesita (el almacén reparte los eventos). **Solo la limpieza** lo usa: si
el servicio tiene un `LockProvider` de ShedLock, la limpieza corre bajo el bloqueo
`auditoria-outbox-limpieza-<spring.application.name>`; si no, corre sin bloqueo en cada instancia (es idempotente:
solo se repite el trabajo).

### Propiedades

| Propiedad | Por defecto | Qué es |
|---|---|---|
| `auditoria.outbox.enabled` | `false` | Activa el outbox |
| `auditoria.outbox.almacen` | (obligatoria) | `jdbc` o `mongo` |
| `auditoria.outbox.tamano-lote` | `50` | Eventos reclamados por ciclo |
| `auditoria.outbox.intervalo` | `5s` | Pausa entre ciclos |
| `auditoria.outbox.max-intentos` | `10` | Fallos tras los que pasa a `FALLIDO` |
| `auditoria.outbox.backoff-base` | `5s` | Espera tras el primer fallo (se duplica por intento) |
| `auditoria.outbox.backoff-max` | `10m` | Tope de la espera |
| `auditoria.outbox.lease` | `5m` | Tiempo que un relay retiene el lote; tiene que superar a `timeout-ack` |
| `auditoria.outbox.timeout-ack` | `30s` | Espera máxima del ack de Kafka por evento |
| `auditoria.outbox.retencion` | `30d` | Antigüedad de los `ENVIADO` que se borran |
| `auditoria.outbox.intervalo-limpieza` | `1h` | Pausa entre limpiezas (y `lockAtMostFor` del bloqueo) |
| `auditoria.outbox.mongo.coleccion` | `auditoria_outbox_<spring.application.name>` | Colección del outbox (solo Mongo) |

`auditoria.topic` y `auditoria.enabled` siguen valiendo: el outbox usa el mismo `AuditoriaPublisher`, así que
`auditoria.enabled=false` con el outbox activo hace fallar el arranque. Una configuración incoherente (sin `almacen`,
`lease <= timeout-ack`, valores no positivos) también falla al arrancar, nombrando la propiedad.

### Almacén JDBC: SQL de referencia (MySQL 8)

La migración la pone **cada servicio** en su Flyway. El mismo SQL lo usan las pruebas de integración
(`src/test/resources/auditoria_outbox_mysql.sql`):

```sql
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
```

- `id` autoincremental define el orden. `event_id` único impide guardar dos veces el mismo evento.
- Las fechas se guardan en milisegundos y se comparan contra el reloj de la aplicación, no contra `NOW()`; todas las
  instancias de un servicio deben correr con la misma zona horaria de JVM (UTC en los contenedores).
- La limpieza usa `DELETE ... ORDER BY id LIMIT n`, propio de MySQL.

### Almacén Mongo: colección e índices

- Colección `auditoria_outbox_<spring.application.name>` (o `auditoria.outbox.mongo.coleccion`). **Nunca la compartan
  dos servicios**, aunque usen la misma base: negociador y liquidador ya comparten `DB_Facturas`, y el nombre por
  defecto lleva el servicio justamente por eso.
- Índices, creados al arrancar por `crearIndices()` (idempotente; el usuario de Mongo necesita permiso `createIndex`):

| Índice | Campos | Para qué |
|---|---|---|
| `ux_auditoria_outbox_event_id` | `{eventId: 1}` único | Un evento, una vez |
| `ix_auditoria_outbox_lote` | `{estado: 1, proximoIntento: 1, fechaCreacion: 1, _id: 1}` | Reclamo del lote |
| `ix_auditoria_outbox_clave` | `{claveKafka: 1, estado: 1, fechaCreacion: 1}` | Orden por clave |
| `ix_auditoria_outbox_limpieza` | `{estado: 1, fechaEnvio: 1}` | Limpieza |

- El orden es `fechaCreacion` y después `_id`.
- Para que el evento y el cambio de negocio sean atómicos, el servicio necesita un **`MongoTransactionManager`** y Mongo
  en **replica set** (también de un nodo). Sin transacción de Mongo, el insert del outbox se confirma solo: el evento
  queda duradero, pero ya no atado al cambio de negocio (ventana de escritura doble, menor que la de hoy).

### Guía de adopción por servicio

Pasos comunes:

1. Subir `ms-commons-util` a `0.5.0`.
2. Agregar `auditoria.outbox.enabled=true` y `auditoria.outbox.almacen=jdbc|mongo` (por ambiente; se puede empezar
   apagado).
3. Hacer que el adaptador de auditoría del servicio delegue en el `AuditoriaPort` de commons cuando existe, y mantener
   el camino actual cuando no:

   ```java
   @Component
   @RequiredArgsConstructor
   public class AuditoriaKafkaAdapter implements AuditoriaPort {          // el puerto propio del servicio

       private final ObjectProvider<com.factoring.commons.auditoria.AuditoriaPort> outbox;
       private final ObjectProvider<AuditoriaPublisher> publicador;

       @Override
       public void registrar(final AuditoriaAccionEvent evento) {
           final com.factoring.commons.auditoria.AuditoriaPort conOutbox = outbox.getIfAvailable();
           if (conOutbox != null) {
               conOutbox.registrar(evento);   // dentro de la transacción del caso de uso; si falla, propaga
               return;
           }
           // ... camino 0.4.x: afterCommit o en el momento
       }
   }
   ```

4. La llamada a `registrar` tiene que quedar **dentro** de la transacción del caso de uso (el mismo `@Transactional`
   que guarda el cambio de negocio).
5. Pruebas del servicio: rollback sin evento, commit con evento, y el arranque con la propiedad activa.

| Servicio | Almacén | Qué añade |
|---|---|---|
| ms-loadfiles-neg | `jdbc` | Migración Flyway con la tabla de arriba; propiedad; adaptador que delega |
| ms-registroempresa-neg | `jdbc` | Igual que loadfiles |
| ms-cargaFacturas-neg | `mongo` | El cambio auditado está en Mongo: `MongoTransactionManager` (replica set) y que el caso de uso corra en esa transacción. Sin replica set, documentar la ventana de escritura doble. Ya tiene ShedLock: la limpieza lo usa sola |
| ms-postliquidacion-util | `mongo` | Igual que cargaFacturas; Mongo es su única base |

Los servicios que ya tienen su propio outbox en `outbox_event` (security, negociador, liquidador) no se tocan en
0.5.0. Migrarlos al relay de commons es un trabajo aparte.

### Operación

- **`FALLIDO` bloquea su clave.** Los eventos siguientes de la misma entidad no salen hasta revisarlo (es lo que
  conserva el orden). Para reintentarlo, devolverlo a `PENDIENTE` con `intentos = 0` y `proximo_intento` en el pasado;
  si se decide descartarlo, borrarlo. Cada `FALLIDO` deja un log `ERROR` `[AUDITORIA][OUTBOX_FALLIDO]`.
- Métricas mínimas que conviene vigilar: cantidad de `PENDIENTE` con `proximo_intento` vencido, cantidad de `FALLIDO`,
  y antigüedad del `PENDIENTE` más viejo.

### Limitaciones conocidas

- El orden por clave es el orden **de inserción visible**. Si dos transacciones concurrentes auditan la misma entidad
  sin bloquearla, la que confirma primero puede salir primero aunque se haya insertado después. Los casos de uso que
  necesiten orden estricto por entidad deben bloquear la entidad (como ya hace security con `lockForUpdate`).
- Un evento sale como mínimo una vez: puede repetirse si el relay cae entre el ack y el marcado, y el consumidor lo
  descarta por `eventId`.
- El almacén JDBC está escrito y probado para MySQL 8 (`SKIP LOCKED`, `DELETE ... LIMIT`).

### Pruebas

- Unitarias del relay (`OutboxAuditoriaRelaySpec`): lote vacío, éxito, fallo con backoff, tope de intentos a `FALLIDO`,
  lease vencido, orden por clave con fallo en medio, error saneado sin payload, payload ilegible, lease agotado.
- Integración con Testcontainers (`JdbcAuditoriaOutboxIntegracionSpec` contra MySQL 8,
  `MongoAuditoriaOutboxIntegracionSpec` contra Mongo 7 en replica set de un nodo, Kafka real `apache/kafka`):
  transacción confirmada y revertida, sin transacción, `eventId` repetido, dos relays concurrentes sin duplicar y en
  orden por clave, caída entre reclamar y publicar, Kafka caído (pausado) y su vuelta, limpieza, `SKIP LOCKED`.
- `AuditoriaOutboxAutoConfigurationSpec`: con `enabled=false` el contexto tiene exactamente los mismos beans que sin la
  autoconfiguración del outbox.
- Necesitan Docker. En una máquina con Docker Desktop cuyo `docker.host` no sea `localhost`, exportar
  `TESTCONTAINERS_HOST_OVERRIDE=localhost` antes de correr `./gradlew check`.
