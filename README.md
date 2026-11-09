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
| `com.factoring.commons.outbox` | Outbox genérico: sobre `EventoOutbox`, `TiposDeEvento`, `OutboxPort` y publicador con cabeceras (desde 0.6.0); `eventId` propio y formato compatible con `JsonSerializer` (desde 0.6.1) |
| `com.factoring.commons.outbox.idempotencia` | `ConsumoIdempotente`: efecto a lo sumo una vez por consumidor y evento (`consumo-idempotente.*`, desde 0.6.0) |

Todas las dependencias de Spring, Kafka, JDBC, Mongo y ShedLock son `compileOnly`: cada pieza se registra solo si el
servicio trae la librería correspondiente.

La definición de terminado de la plataforma está en [DEFINITION_OF_DONE.md](DEFINITION_OF_DONE.md).

---

## 0.6.1: `eventId` propio y formato compatible con `JsonSerializer`

### Compatibilidad

Un servicio que sube de 0.6.0 (o de 0.5.1) sin usar lo nuevo sigue exactamente igual: `registrar(tipo, payload)`
genera el UUID como antes, todos los tipos salen como sobre y no cambia ninguna propiedad, tabla ni colección. Lo
nuevo se activa en código: con la sobrecarga de `registrar` o con una opción en la declaración del tipo. La suite de
0.6.0 pasa sin modificaciones.

Cambios visibles en la API (todos aditivos):
- `OutboxPort` gana `registrar(tipo, eventId, payload)` como método `default`. En una implementación propia que no
  lo sobrescriba, lanza `UnsupportedOperationException`.
- `AuditoriaOutboxPort` gana `guardarSiNoExiste(evento)` como método `default`, que delega en `guardar`.
- `TipoDeEvento` gana el componente `idTipoCompatible`, al final. Conserva el constructor de 5 argumentos.
- `KafkaPublicadorOutbox` gana un constructor que recibe los `TiposDeEvento`; el de 0.6.0 sigue.
- `AuditoriaOutboxMetricas` gana un constructor con el contador de duplicados.
- Métrica nueva: `outbox.duplicados`.

### `eventId` propio

```java
outbox.registrar("documento.creado", documento.id(), new DocumentoCreado(documento.id(), ...));
// Optional<EventoOutbox<DocumentoCreado>>: vacío si ese eventId ya estaba en el outbox
```

- El valor que pasa el productor (por ejemplo el id del agregado) es a la vez el `eventId` del sobre guardado, el de
  la fila o documento (`event_id` / `eventId`) y el de la cabecera `eventId` del mensaje.
- Tiene que tener entre 1 y 64 caracteres (`EventoOutbox.LONGITUD_MAXIMA_EVENT_ID`, el ancho de la columna
  `event_id`) y no puede estar en blanco: si no, `IllegalArgumentException` y no se guarda nada.
- Se aplican las mismas reglas que en `registrar(tipo, payload)`: tipo declarado, clase del payload, clave no vacía y
  modo "solo drenar".

**`eventId` repetido en el mismo almacén.** Si ya hay un evento con ese `eventId`:
- no se escribe un segundo evento, y el que estaba no cambia;
- **no falla la transacción de negocio**: `registrar` devuelve vacío y el caso de uso puede seguir y confirmar;
- se cuenta en `outbox.duplicados` (tags `tipo` y `servicio`) y queda un WARN con el tipo y el `eventId`.

Por qué: con un id de negocio, un segundo registro del mismo `eventId` es casi siempre un reintento del mismo caso de
uso, y el evento que se quiere ya está guardado. Hacer fallar la transacción revertiría trabajo correcto. Publicar dos
veces el mismo `eventId` con contenidos distintos sería peor, porque el consumidor descarta el segundo por `eventId`.
El contador hace visible el caso para revisarlo si no se esperaba.

Cómo se consigue en cada almacén:
- **JDBC (MySQL):** el INSERT choca con el índice único de `event_id`. En MySQL eso revierte solo la sentencia, no la
  transacción.
- **Mongo:** dentro de una transacción, un error de clave duplicada la abortaría entera. Por eso se escribe con un
  upsert por `eventId` y `$setOnInsert`, que no cambia nada si el documento existe.
- **Registros simultáneos** del mismo `eventId` en dos transacciones abiertas a la vez:
  - en MySQL, la segunda espera el bloqueo del índice único hasta que la primera termine; entonces ve el duplicado
    (si la primera confirmó) o inserta (si revirtió);
  - en Mongo, la segunda falla con un conflicto de escritura (`WriteConflict`, error transitorio), como cualquier
    conflicto de su transacción, y se puede reintentar.

  Este caso no está cubierto por las pruebas automáticas.
- **Rollback:** si la transacción que registró el evento se revierte, el `eventId` queda libre para el reintento.

### Formato compatible con `JsonSerializer`, por tipo

Para migrar al outbox un productor que hoy publica con el `JsonSerializer` de Spring Kafka, sin tocar a sus
consumidores (que leen con `JsonDeserializer` y la cabecera `__TypeId__`):

```java
TipoDeEvento.de("documento.creado", DocumentoCreado.class)
        .topic("${topics.documento-creado}")
        .clave(DocumentoCreado::documentoId)
        .compatibleConJsonSerializer("documentoCreado")   // alias del spring.json.type.mapping del consumidor
        // .compatibleConJsonSerializer()                 // o el nombre completo de la clase
        .build();
```

Un tipo así sale con:
- **valor:** solo el payload, con los mismos bytes que escribe un `JsonSerializer` por defecto (el `ObjectMapper` de
  `JacksonUtils` de Spring Kafka);
- **cabeceras:** las del sobre (`eventId`, `tipo`, `version`, `servicioOrigen`) y `__TypeId__` con el valor
  declarado;
- **clave:** la del tipo, como siempre.

Detalles:
- Los tipos sin la opción siguen saliendo como sobre, sin `__TypeId__`. Por defecto no hay ninguno compatible.
- En el almacén se guarda siempre el sobre completo; la conversión la hace `KafkaPublicadorOutbox` al publicar. La
  clase del payload tiene que poder reconstruirse desde su propio JSON (un record o un POJO con Jackson).
- Si el productor anterior usaba un `JsonSerializer` con un `ObjectMapper` propio, los bytes pueden diferir. En ese
  caso, o para cualquier otro formato, se declara un bean `PublicadorOutbox` propio: el de la librería es
  `@ConditionalOnMissingBean`.
- Con `eventId` propio, la cabecera `eventId` del mensaje lleva ese valor.

### Pruebas de 0.6.1

Las de integración corren contra MySQL 8, Mongo 7 en replica set y un broker Kafka reales (Testcontainers):
- `JdbcEventIdPropioIntegracionSpec` y `MongoEventIdPropioIntegracionSpec`, sobre la base común `EventIdPropioBaseSpec`:
  - el `eventId` propio es el mismo en la fila, en el sobre y en la cabecera;
  - un `eventId` repetido, en la misma transacción y en otra, no escribe un segundo evento, el negocio confirma y se
    cuenta;
  - después de un rollback, el reintento registra el evento;
  - se valida el `eventId`: vacío, en blanco, nulo o de más de 64 caracteres;
  - el valor de un tipo compatible es idéntico byte a byte al del `JsonSerializer`, con `__TypeId__` (alias y nombre
    de clase) y las cabeceras del sobre;
  - un tipo no compatible sale como sobre;
  - un consumidor real con `JsonDeserializer`, mapeo de alias y paquetes de confianza lee el mensaje sin nada
    especial.
- `OutboxCompatibilidadSpec`: la declaración de los tipos, los puertos por defecto y la autoconfiguración (el
  publicador recibe los tipos y se publica `outbox.duplicados`).

---

## Outbox genérico, métricas por tipo y consumo idempotente (0.6.0)

### Qué cambia y qué no

0.6.0 generaliza el outbox de 0.5.x: además de los eventos de auditoría, guarda y publica **cualquier tipo de evento
que el servicio declare**, con un sobre común y cabeceras Kafka. Suma un helper de **consumo idempotente** para los
consumidores.

**Compatible con 0.5.1.** Un servicio que sube sin tocar su configuración sigue igual:
- mismas propiedades `auditoria.outbox.*`, la tabla `auditoria_outbox` y la colección `auditoria_outbox_<servicio>`;
- el mismo formato de los eventos de auditoría;
- los mismos constructores públicos.

Cambios visibles:
- `AuditoriaOutboxEvento` gana el componente `tipo`, al final y nulo en los eventos de auditoría. Conserva el
  constructor de 12 argumentos.
- `AuditoriaOutboxPort` gana dos métodos `default`: `resumenPorTipo()` y `verificarEventosGenericos()`.
- `JdbcAuditoriaOutboxAdapter` pasa a ser `final`.
- Los gauges se refrescan en su propio hilo.
- Hay gauges nuevos `outbox.*`; los de 0.5.x quedan como alias.
- Mongo crea dos índices nuevos al arrancar: `ix_outbox_orden` y el TTL `ttl_outbox_enviados`.

### Sobre del evento

`EventoOutbox<T>` (JSON de `OutboxJson`) es el valor del mensaje Kafka:

| Campo | Qué es |
|---|---|
| `eventId` | UUID que genera el productor al registrar el evento, dentro de su transacción, o el id propio que pasa el productor (desde 0.6.1). Es la clave de idempotencia de punta a punta: el relay y los reintentos no lo cambian |
| `tipo`, `version` | Nombre lógico estable del evento y versión del esquema del payload |
| `servicioOrigen` | `spring.application.name` del productor |
| `ocurridoEn` | Instante UTC del registro |
| `requestId` | MDC `requestId`, para correlacionar con los logs |
| `clave` | Clave Kafka y de orden, calculada por el tipo a partir del payload |
| `payload` | Los datos del evento |

Cabeceras Kafka: `eventId`, `tipo`, `version` y `servicioOrigen`. Así un consumidor puede deduplicar y enrutar sin
leer el payload. No van headers de tipo de Spring (`__TypeId__`).

### Declarar los tipos y registrar

Cada servicio expone un bean `TiposDeEvento`, en código, para que el compilador vigile la clase y la función de la
clave:

```java
@Bean
TiposDeEvento tiposDeEvento() {
    return TiposDeEvento.de(
            TipoDeEvento.de("documento.creado", DocumentoCreado.class)
                    .version(1)
                    .topic("${topics.documento-creado}")   // placeholder de una propiedad, o el nombre directo
                    .clave(DocumentoCreado::documentoId)    // nunca un valor aleatorio
                    .build());
}
```

```java
@Transactional
public void crear(...) {
    repositorio.guardar(documento);
    outbox.registrar("documento.creado", new DocumentoCreado(documento.id(), ...));   // OutboxPort
}
```

- **Al arrancar** (fail-fast) falla si:
  - hay un nombre repetido o vacío, una versión menor que 1, o falta el topic o la función de clave;
  - un topic queda sin resolver;
  - el almacén no admite eventos genéricos (una tabla JDBC sin la columna `tipo`).
- **En ejecución** `registrar` rechaza:
  - un tipo no declarado (`TipoDeEventoNoDeclaradoException`);
  - un payload de otra clase;
  - una clave vacía.
- **Orden:** se garantiza por `clave` dentro de un servicio, con el mismo bloqueo por clave del relay de 0.5.1. Entre
  servicios no hay orden.

### Almacén por servicio

La tabla o la colección es **propia de cada servicio**, nunca compartida. Si dos servicios comparten una tabla, cada
relay drena eventos del otro y los gauges suman los dos.

| Propiedad | Default | Qué es |
|---|---|---|
| `auditoria.outbox.jdbc.tabla` | `auditoria_outbox` | Tabla del almacén JDBC (p. ej. `outbox_<servicio>`); solo letras, dígitos y `_` |
| `auditoria.outbox.mongo.coleccion` | `auditoria_outbox_<servicio>` | Colección del almacén Mongo (ya existía) |
| `auditoria.outbox.solo-drenar` | `false` | Ver "Volver atrás: solo drenar" |

Esquema JDBC 0.6.0: el de 0.5.x más la columna `tipo`. Una tabla nueva:

```sql
CREATE TABLE outbox_mi_servicio (
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
```

Una tabla de 0.5.x que vaya a guardar también eventos genéricos necesita
`ALTER TABLE <tabla> ADD COLUMN tipo VARCHAR(100) NULL`. Solo con auditoría no hace falta: el adaptador detecta si la
columna existe.

### Relay

Es el mismo relay de 0.5.1, con los mismos estados, lease, ack con tiempo límite, backoff, FALLIDO que bloquea su
clave y limpieza. Solo cambia según el tipo de evento:
- **Con `tipo`:** sale por `PublicadorOutbox`, el sobre tal cual con sus cabeceras. `KafkaPublicadorOutbox` usa un
  productor propio con las propiedades `spring.kafka.*` del servicio, `acks=all` e idempotencia.
- **De auditoría:** sigue saliendo por `AuditoriaPublisher`, sin cambios.

### Volver atrás: solo drenar

`auditoria.outbox.solo-drenar=true` deja el relay publicando lo pendiente, pero el outbox **no acepta eventos nuevos**:
- `OutboxPort.registrar` lanza `OutboxSoloDrenarException`; quien registra elige su camino alternativo;
- los eventos de auditoría se publican directo, como sin outbox.

Lo ya registrado no se pierde. Para pasar los pendientes de una tabla o colección a otra, hay que detener antes el
relay de ambas (o copiar y borrar en una sola operación): así ningún evento sale dos veces ni queda sin salir.

### Consumidores: orden por clave y duplicados

El outbox entrega **al menos una vez**: un evento puede llegar repetido, siempre con el mismo `eventId`. El orden por
clave se conserva de punta a punta solo si el consumidor procesa **de forma bloqueante**: con el `DefaultErrorHandler`
de Spring Kafka, mientras un mensaje se reintenta los siguientes de su partición esperan.

Con reintentos no bloqueantes (`@RetryableTopic` y topics de reintento), o al mandar un mensaje a la DLT, el siguiente
evento de la misma clave se procesa antes que el fallido, y **se pierde el orden**. La DLT queda para errores que no se
pueden reintentar, y conviene que su procesamiento aplique las mismas guardas de estado.

### Métricas (#15)

El refresco de las métricas corre en **su propio hilo** (`outbox-metricas`), separado del relay. Un relay esperando un
ack, hasta `timeout-ack` por evento, ya no congela los gauges.

| Gauge | Tags | Qué mide |
|---|---|---|
| `outbox.pendientes` | `tipo`, `servicio` | PENDIENTE y EN_PROCESO |
| `outbox.en_proceso` | `tipo`, `servicio` | EN_PROCESO: reclamados por un relay, con lease |
| `outbox.fallidos` | `tipo`, `servicio` | FALLIDO; cada uno bloquea los posteriores de su clave: alertar si es mayor que 0 |
| `outbox.edad_mas_antiguo_segundos` | `tipo`, `servicio` | Segundos que lleva esperando el pendiente más antiguo |
| `auditoria.outbox.pendientes`, `auditoria.outbox.fallidos`, `auditoria.outbox.edad_mas_antiguo_segundos` | `servicio` | Alias de 0.5.x con el total de todos los tipos; se mantienen durante 0.6.x |

Los eventos de auditoría se cuentan con `tipo=auditoria.accion`.

### Mongo: `tomarLote` con claves bloqueadas (#14)

En 0.5.1, `tomarLote` ordenaba los candidatos en memoria y evaluaba un `$lookup` por documento antes del límite. En
0.6.0:
- recorre los candidatos **por páginas** sobre el índice nuevo `{estado, fechaCreacion, _id}`, con el orden servido
  por el índice y una proyección sin `payload`;
- para cada página calcula, con un solo `$group`, el primer bloqueante de cada una de sus claves: FALLIDO, EN_PROCESO
  o PENDIENTE en backoff;
- descarta en memoria los candidatos posteriores a su bloqueante y para al llenar el lote;
- reclama los elegibles con **un solo `updateMany`** condicionado a `PENDIENTE` y marcado con un token de la llamada, en
  vez de un `findAndModify` por evento, que esperaba una escritura cada vez. Si otro relay se llevó antes alguno, los
  siguientes de esa clave se liberan, igual que antes.

Una clave bloqueada no ocupa el lote y las demás siempre salen.

**Limpieza:** con `auditoria.outbox.retencion` (30 días por defecto), Mongo crea además un **índice TTL parcial**
(`ttl_outbox_enviados`) sobre `fechaEnvio` de los ENVIADO. Si la retención cambia, se ajusta con `collMod` al arrancar.
La limpieza periódica sigue activa.

**Medición:** mismo escenario de la issue #14, con los adaptadores reales contra MySQL 8 y Mongo 7 (replica set de un
nodo) locales y `./gradlew medirTomarLote`:
- 100 000 eventos en 2 000 claves de 50;
- 40 claves con un FALLIDO en cabeza y 20 con la cabeza en backoff, todas primero en el orden de creación;
- 20 ciclos de `tomarLote(50)`.

| Almacén | 0.5.1: p95 / mediana / máximo | 0.6.0: p95 / mediana / máximo |
|---|---|---|
| Mongo 7.0.34 | 804 / 644 / 1021 ms | **64** / 48 / 162 ms |
| JDBC (MySQL 8.0.46) | 88 / 46 / 95 ms | 46 / 35 / 128 ms |

En los 20 ciclos de cada corrida se tomaron 50 eventos y ninguno de claves bloqueadas. El máximo es el primer ciclo, en
frío.

### Consumo idempotente

`ConsumoIdempotente.procesar(consumidor, eventId, efecto)` aplica el efecto **a lo sumo una vez por consumidor**: inserta
la marca `(consumidor, eventId)` en la **misma transacción** que el efecto.
- Si la marca ya existe, el efecto no corre y devuelve `false`.
- Si el efecto falla, se revierten la marca y el efecto, y el reintento lo vuelve a aplicar.
- Sin `eventId` (productores que todavía no lo envían) se usa la variante `procesar(consumidor, eventId, claveNatural,
  efecto)`, que guarda la clave natural con el prefijo `nat:`.

**Solo sirve si el efecto está en el mismo almacén que la marca**, con la misma base y el mismo gestor de
transacciones. Para un efecto externo (un correo, una llamada HTTP) no hay transacción común. Ahí hay dos caminos:
- marcar **después** del efecto y aceptar un duplicado raro;
- si un duplicado es inaceptable, reclamar el efecto antes con estados propios (en curso, hecho, incierto) y no
  reintentarlo solo cuando quedó a medias.

| Almacén | Marca | Retención | Concurrencia |
|---|---|---|---|
| `jdbc` | Tabla `evento_procesado` con PK `(consumidor, clave)`. Usa el gestor de transacciones del servicio (REQUIRED) | Limpieza periódica en un hilo propio (`intervalo-limpieza`) | El segundo INSERT espera el bloqueo de la PK; si la primera transacción confirma, el evento es un duplicado |
| `mongo` | Colección `evento_procesado` con `_id = {consumidor, clave}`. Usa un `MongoTransactionManager` (replica set) | Índice TTL sobre `fechaProcesado` | Una transacción gana; la otra recibe un conflicto transitorio que el consumidor reintenta, y el reintento ya ve la marca |

```sql
CREATE TABLE IF NOT EXISTS evento_procesado (
    consumidor      VARCHAR(100) NOT NULL,
    clave           VARCHAR(191) NOT NULL,
    fecha_procesado DATETIME(6)  NOT NULL,
    PRIMARY KEY (consumidor, clave),
    KEY ix_evento_procesado_fecha (fecha_procesado)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
```

| Propiedad | Default | Qué es |
|---|---|---|
| `consumo-idempotente.enabled` | `false` | Activa el consumo idempotente |
| `consumo-idempotente.almacen` | (obligatorio) | `jdbc` o `mongo` |
| `consumo-idempotente.retencion` | `30d` | Cuánto se conserva cada marca |
| `consumo-idempotente.intervalo-limpieza` | `1h` | Pausa entre limpiezas (JDBC) |
| `consumo-idempotente.tabla` / `coleccion` | `evento_procesado` | Dónde se guardan las marcas |

**Retención.** La protección principal contra un efecto repetido son las **guardas de estado del propio dominio**: no
repetir una transición que ya ocurrió. La marca es la segunda barrera, para los duplicados que llegan antes de que el
estado cambie o cuando no hay estado que mirar.

Por eso la retención tiene que ser **mayor que** dos cosas:
- la retención de los topics que se consumen, porque un consumidor que rebobina offsets reentrega todo lo que el topic
  guarda;
- lo que tarde en resolverse a mano un evento fallido del outbox que se reintenta con el mismo `eventId`.

30 días cubren la retención por defecto de Kafka (7 días) y un plazo de resolución manual de dos semanas. Si un topic
guarda más tiempo, hay que subirla.

### Pruebas de 0.6.0

Todas con Testcontainers: MySQL 8, Mongo 7 en replica set de un nodo y Kafka.
- **Outbox genérico**, en los dos almacenes:
  - transacción confirmada y revertida;
  - publicación real en Kafka con clave y cabeceras;
  - auditoría y genéricos juntos, con el resumen por tipo;
  - 30 claves bloqueadas al principio, con más de una página de candidatos: el lote se llena solo con las claves
    libres;
  - "solo drenar";
  - métricas que se siguen actualizando con `timeout-ack` de 30 s mientras el relay espera un ack.
- **Esquema:** tabla de 0.5.x sin la columna `tipo` y nombres de tabla inválidos; índices y TTL en Mongo.
- **Consumo idempotente**, en los dos almacenes:
  - duplicado secuencial;
  - marca por consumidor;
  - efecto que falla y se reintenta;
  - clave natural `nat:`;
  - 8 entregas concurrentes que aplican el efecto una sola vez;
  - limpieza por retención.
- **Autoconfiguración:**
  - `OutboxPort` con topics resueltos, y fail-fast si falta un topic o el esquema no sirve;
  - `jdbc.tabla` y `solo-drenar`;
  - gauges por tipo y alias;
  - `consumo-idempotente`.
- La suite de 0.5.1 corre sin cambios, salvo el test que simulaba otro relay adelantándose: ahora intercepta el
  `updateMany` del reclamo en lugar del `findAndModify`.

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
   - Mongo: desde 0.6.0, candidatos por páginas en orden de índice con el primer bloqueante de cada clave calculado
     por página, y un solo `updateMany` condicionado a `estado = PENDIENTE` (ver la sección 0.6.0, #14). En 0.5.x era
     una agregación con `$lookup` y un `findAndModify` por documento.
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
| `ix_outbox_orden` (0.6.0) | `{estado: 1, fechaCreacion: 1, _id: 1}` | Candidatos del lote en orden, sin ordenar en memoria |
| `ttl_outbox_enviados` (0.6.0) | `{fechaEnvio: 1}` TTL, parcial a `estado: ENVIADO` | Borra los ENVIADO al cumplir `auditoria.outbox.retencion` |

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
