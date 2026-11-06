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

## Outbox de auditoría (0.5.0; corregido en 0.5.1)

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
2. **Reclamo del lote**, en orden de creación, con lease `ahora + auditoria.outbox.lease`. Los candidatos son los
   `PENDIENTE` listos (`proximoIntento <= ahora`) **sin un anterior de su clave que no pueda ir en este lote**: un
   `FALLIDO`, un `EN_PROCESO` o un `PENDIENTE` todavía en backoff. Ese filtro se aplica **antes** del límite del lote,
   así que una clave bloqueada nunca ocupa el lote de las demás (ver «Corrección 0.5.1»):
   - JDBC: transacción propia y corta en READ COMMITTED con `SELECT ... WHERE NOT EXISTS (anterior bloqueante) ...
     FOR UPDATE SKIP LOCKED`; el `NOT EXISTS` usa el índice `(clave_kafka, estado, id)` y no bloquea filas. La
     publicación ocurre **fuera** de esa transacción.
   - Mongo: agregación con `$lookup` sobre la misma colección (el mismo filtro) y después `findAndModify` documento a
     documento, condicionado a `estado = PENDIENTE`.
   - Dos relays nunca reciben el mismo evento.
3. **Orden por clave Kafka.** Un evento solo se toma si todos los anteriores no enviados de su misma `claveKafka`
   (`<entidad.tipo>:<entidad.id>`) también están en el lote (un anterior pendiente y listo puede ir en el mismo lote).
   Ante el primer fallo de una clave, los siguientes de esa clave en el lote se liberan sin publicar.
4. **Publicación** con `publicarConfirmado` y espera acotada (`auditoria.outbox.timeout-ack`). Solo tras el ack se marca
   `ENVIADO`. Si falla: `intentos + 1` y `proximoIntento = ahora + backoff`, con
   `backoff = backoff-base * 2^(intentos-1)` y tope `backoff-max`. Al llegar a `max-intentos` pasa a `FALLIDO` con un log
   `ERROR` `[AUDITORIA][OUTBOX_FALLIDO]` que lleva id, `eventId`, clave, intentos y causa, **nunca el payload**.
5. Si al relay ya no le queda lease para esperar un ack completo, libera el resto del lote sin publicarlo.
6. **Marcado tras el ack.** Si `marcarEnviado` falla después de que Kafka confirmó, se reintenta en el momento hasta 3
   veces (esperas de 200 ms y 400 ms). Si aun así falla, un log `WARN` `[AUDITORIA][OUTBOX_SIN_MARCAR]` avisa que el
   evento **ya fue entregado a Kafka** y que saldrá duplicado cuando venza su lease (el consumidor lo descarta por
   `eventId`).

### Corrección 0.5.1: una clave bloqueada ya no frena todo el outbox

En 0.5.0 el lote se cortaba (`LIMIT tamano-lote`) **antes** de aplicar el orden por clave. Si los primeros
`tamano-lote` pendientes eran sucesores de un evento bloqueado (un `FALLIDO`, uno en backoff o uno `EN_PROCESO`), el lote
quedaba vacío y el relay no publicaba nada, aunque hubiera eventos de otras claves esperando: un solo `FALLIDO` (un
payload ilegible lo es al primer intento) con `tamano-lote` o más eventos detrás detenía todo el outbox del servicio. Los
dos almacenes lo tenían. En 0.5.1 el filtro va dentro de la consulta de candidatos, antes del límite.

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
| `auditoria.outbox.intervalo-metricas` | `30s` | Pausa entre lecturas del almacén para las métricas (solo con Micrometer) |
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
- Las fechas se guardan en milisegundos y **en UTC**, y se comparan contra el reloj de la aplicación, no contra
  `NOW()` (ver «Tiempo en UTC»).
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

- El orden es `fechaCreacion` y después `_id`. Mongo guarda las fechas en UTC siempre (tipo `Date`).
- Para que el evento y el cambio de negocio sean atómicos, el servicio necesita un **`MongoTransactionManager`** y Mongo
  en **replica set** (también de un nodo). Sin transacción de Mongo, el insert del outbox se confirma solo: el evento
  queda duradero, pero ya no atado al cambio de negocio (ventana de escritura doble, menor que la de hoy).

### Guía de adopción por servicio

Pasos comunes:

1. Subir `ms-commons-util` a `0.5.1` (no a 0.5.0: ver «Corrección 0.5.1»).
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
0.5.x. Migrarlos al relay de commons es un trabajo aparte.

#### Dependencias que tiene que traer cada servicio

Todas las dependencias de commons son `compileOnly`: el servicio las pone en tiempo de ejecución.

| Para | Dependencias (o el starter que las trae) |
|---|---|
| Siempre (auditoría) | `spring-boot-autoconfigure`, `spring-kafka`, `jackson-databind`, `jackson-datatype-jsr310`, `slf4j-api` (los traen `spring-boot-starter` + `spring-kafka`) |
| Almacén `jdbc` | `spring-jdbc` y `spring-tx` (`spring-boot-starter-jdbc` o `spring-boot-starter-data-jpa`), un `DataSource` y `mysql-connector-j` 8.0.23 o más nuevo |
| Almacén `mongo` | `spring-data-mongodb` y `mongodb-driver-sync` (`spring-boot-starter-data-mongodb`) y un `MongoTemplate`; para atomicidad, un `MongoTransactionManager` con replica set |
| Bloqueo de la limpieza (opcional) | `shedlock-core` (lo trae `shedlock-spring`) y un bean `LockProvider` |
| Métricas (opcional) | `micrometer-core` y Spring Boot Actuator (`spring-boot-starter-actuator`) |

### Tiempo en UTC

Los cuatro instantes del outbox (`proximo_intento`, `lease_hasta`, `fecha_creacion`, `fecha_envio`) se guardan y se
comparan en **UTC**, sin depender de la zona de la JVM ni de la de la sesión de MySQL:

- JDBC: viajan como `LocalDateTime` en UTC. Connector/J (8.0.23 o más nuevo) no convierte `LocalDateTime` ni con la zona
  de la JVM ni con la de la sesión, y `DATETIME` no tiene zona. **No hace falta ningún parámetro de conexión**
  (`connectionTimeZone`, `preserveInstants`, `forceConnectionTimeZoneToSession`): los valores por defecto sirven.
  Al consultar a mano, comparar contra `UTC_TIMESTAMP(3)`, no contra `NOW()`.
- Mongo: `Date` es UTC siempre.
- Las pruebas de integración corren con la JVM en UTC y otra vez en `America/Bogota` (`testIntegracionBogota`), y la
  sesión de MySQL de las pruebas está en `+05:00`.

### Métricas

Si el servicio tiene Micrometer y Actuator, se registran tres gauges con el tag `servicio=<spring.application.name>`:

| Gauge | Qué mide | Alerta sugerida |
|---|---|---|
| `auditoria.outbox.pendientes` | Eventos `PENDIENTE` y `EN_PROCESO` | Crece sin bajar |
| `auditoria.outbox.fallidos` | Eventos `FALLIDO` | **Mayor que 0**: hay una clave detenida |
| `auditoria.outbox.edad_mas_antiguo_segundos` | Segundos que lleva esperando el pendiente más antiguo | Por encima de unos minutos |

Leer las gauges no consulta la base: el planificador lee el almacén cada `intervalo-metricas`, y solo si las gauges
están registradas. Sin Micrometer no se registra nada y no hay consultas de más. El log `ERROR`
`[AUDITORIA][OUTBOX_FALLIDO]` de cada `FALLIDO` se mantiene.

### Runbook: eventos FALLIDO

> **Un `FALLIDO` bloquea todos los eventos posteriores de su misma clave Kafka** (la misma entidad). No se
> publican hasta resolverlo; los de las demás claves siguen saliendo.

Un evento pasa a `FALLIDO` al agotar `max-intentos` (Kafka caído mucho tiempo, topic inexistente) o al primer intento si
su payload no se puede leer (`ultimoError = PAYLOAD_ILEGIBLE`). El payload lleva ip, actor y detalle: no copiarlo a
canales abiertos.

**MySQL** (tabla `auditoria_outbox`):

```sql
-- Ver los FALLIDO (sin el payload) y cuántos eventos tiene detenidos cada uno detrás
SELECT id, event_id, clave_kafka, intentos, ultimo_error, fecha_creacion
FROM auditoria_outbox WHERE estado = 'FALLIDO' ORDER BY id;

SELECT f.id, f.clave_kafka, COUNT(p.id) AS detenidos
FROM auditoria_outbox f
JOIN auditoria_outbox p ON p.clave_kafka = f.clave_kafka AND p.id > f.id AND p.estado = 'PENDIENTE'
WHERE f.estado = 'FALLIDO' GROUP BY f.id, f.clave_kafka;

-- Reintentar uno (después de corregir la causa): vuelve a la cola con intentos en 0
UPDATE auditoria_outbox
SET estado = 'PENDIENTE', intentos = 0, proximo_intento = UTC_TIMESTAMP(3), ultimo_error = NULL
WHERE id = :id AND estado = 'FALLIDO';

-- Descartarlo (decisión explícita: el evento no llegará a la auditoría). Guardar antes el payload en el mismo
-- destino restringido que el logger AUDITORIA_NO_PUBLICADO.
DELETE FROM auditoria_outbox WHERE id = :id AND estado = 'FALLIDO';
```

**Mongo** (colección `auditoria_outbox_<servicio>`, desde `mongosh`):

```javascript
// Ver los FALLIDO, sin el payload
db.getCollection("auditoria_outbox_<servicio>")
  .find({ estado: "FALLIDO" }, { payload: 0 }).sort({ fechaCreacion: 1, _id: 1 })

// Reintentar uno
db.getCollection("auditoria_outbox_<servicio>").updateOne(
  { _id: ObjectId("<id>"), estado: "FALLIDO" },
  { $set: { estado: "PENDIENTE", intentos: 0, proximoIntento: new Date(), ultimoError: null } })

// Descartarlo (guardar antes el payload en el destino restringido)
db.getCollection("auditoria_outbox_<servicio>").deleteOne({ _id: ObjectId("<id>"), estado: "FALLIDO" })
```

Al reintentar o descartar el `FALLIDO`, sus sucesores salen en orden en los ciclos siguientes. Si vuelve a fallar, volverá
a `FALLIDO` al agotar los intentos.

### Limitaciones conocidas

- **Orden de inserción visible, no de commit.** El orden por clave es el de inserción tal como lo ve el relay. Si dos
  transacciones concurrentes auditan la misma entidad sin bloquearla, la que confirma primero puede salir primero
  aunque se haya insertado después. El consumidor ordena por `occurredAt` (cuándo ocurrió la acción), no por el orden
  de llegada. Los casos de uso que necesiten orden estricto de publicación por entidad deben bloquear la entidad (como
  ya hace security con `lockForUpdate`).
- **Al menos una vez.** Un evento puede repetirse si el relay cae entre el ack y el marcado, o si el marcado falla tres
  veces seguidas; el consumidor lo descarta por `eventId`.
- **UTC.** El outbox guarda y compara en UTC; quien consulte o edite a mano tiene que usar UTC (`UTC_TIMESTAMP(3)` en
  MySQL).
- El almacén JDBC está escrito y probado para MySQL 8 (`SKIP LOCKED`, `DELETE ... LIMIT`).

### Pruebas

- Unitarias del relay (`OutboxAuditoriaRelaySpec`): lote vacío, éxito, fallo con backoff, tope de intentos a `FALLIDO`,
  lease vencido, orden por clave con fallo en medio, error saneado sin payload, payload ilegible, lease agotado,
  reintentos del marcado tras el ack.
- Integración con Testcontainers (`JdbcAuditoriaOutboxIntegracionSpec` contra MySQL 8,
  `MongoAuditoriaOutboxIntegracionSpec` contra Mongo 7 en replica set de un nodo, Kafka real `apache/kafka`):
  transacción confirmada y revertida, sin transacción, `eventId` repetido, dos relays concurrentes sin duplicar y en
  orden por clave, caída entre reclamar y publicar, Kafka caído (pausado) y su vuelta, limpieza, `SKIP LOCKED`, claves
  bloqueadas que no frenan a las demás (FALLIDO, backoff y EN_PROCESO delante de `tamano-lote + 10` sucesores),
  marcado que falla tras el ack, resumen de métricas y fechas en UTC.
- `AuditoriaOutboxAutoConfigurationSpec`: con `enabled=false` el contexto tiene exactamente los mismos beans que sin la
  autoconfiguración del outbox; gauges con y sin Micrometer.

### Correr los tests en local

Necesitan Docker. Desde la raíz del repo:

```bash
export TESTCONTAINERS_HOST_OVERRIDE=localhost
./gradlew check --no-daemon "-Dorg.gradle.jvmargs=-Xmx768m -Dfile.encoding=UTF-8"
```

- `TESTCONTAINERS_HOST_OVERRIDE=localhost`: con Docker Desktop, si `~/.testcontainers.properties` define un
  `docker.host` que no es `localhost`, los contenedores anuncian una dirección que no sirve (Kafka no arranca).
- `-Dfile.encoding=UTF-8`: las fuentes están en UTF-8 y en Windows la JVM compila por defecto en cp1252.
- Si se desactiva Ryuk (`TESTCONTAINERS_RYUK_DISABLED=true`), las specs paran sus contenedores al terminar; se llaman
  `integrity-e2e-tc-*` para reconocerlos.
- `check` corre `test` (JVM en UTC) y `testIntegracionBogota` (la integración otra vez con la JVM en
  `America/Bogota`).

| Pieza | Versión | Por qué |
|---|---|---|
| Testcontainers | 1.21.4 (fijada en `build.gradle`) | La 1.21.3 del BOM de Spring Boot 3.5.4 no habla con Docker Engine 29 (API mínima 1.44) |
| Kafka | `apache/kafka:3.8.1` | Con la 3.9.0 en modo combinado, el contenedor anuncia el listener del controller en `0.0.0.0` y Kafka no arranca con el `KafkaContainer` de Testcontainers 1.21 |
| MySQL | `mysql:8.0` | El motor objetivo del almacén JDBC |
| Mongo | `mongo:7.0` | Replica set de un nodo (`MongoDBContainer`), necesario para las transacciones |
