# Integrity — Definition of Done

> **Versión canónica.** Este documento vive **solo acá**:
> `ms-commons-util/DEFINITION_OF_DONE.md` — el repo designado para lo compartido
> de la plataforma. Cada repo tiene un `CONTRIBUTING.md` fino que **enlaza a este
> archivo por URL** (no una copia local) y agrega su **bloque de _specifics_**
> (§8). **No dupliques este contenido por repo**: 18 copias que divergen es
> exactamente el problema que este documento existe para evitar. Si algo hay que
> cambiar, se propone contra este archivo y se difunde solo.
>
> Aplica a los **10 backends** (Spring Boot / Gradle / Java 17) y a los
> **8 frontends** (Angular / npm). Donde una regla se materializa distinto por
> stack, la §6 mapea el ítem a la herramienta y el comando concretos.

---

## 1. Propósito y alcance

Este documento define **qué significa que un cambio esté terminado** en la
plataforma Integrity (Factoring / Confirming). Es la lista que corre el autor
antes de pedir revisión y la que corre el revisor antes de aprobar.

- **Se aplica a**: toda rama antes de mergearse a `master` — feature, fix,
  refactor, o cambio de infraestructura que toque código de un repo.
- **Responsables**: el **autor** produce la evidencia; el **revisor** la
  verifica antes de aprobar. Ninguno de los dos "confía en que estará bien".
- **Repos nuevos**: todo repo nuevo (backend o frontend) incluye su
  `CONTRIBUTING.md` — con el link a este documento y el bloque de _specifics_
  (§8) **completado** — desde su **primer commit**, no agregado después. Ver §9.
- **Estado de CI (2026-09)**: varios repos tienen el pipeline de CI roto o
  inexistente. Hasta que se normalice, **la verificación local es la
  autoridad** y el estándar de evidencia de la §5 es obligatorio. No se aprueba
  un cambio "porque compila".

---

## 2. Definition of Done — la checklist

Se corre de arriba a abajo. Cada ítem: **[GATE]** bloquea el merge; **[SHOULD]**
se puede diferir con justificación anotada en el PR.

| # | Ítem | Nivel |
|---|------|-------|
| 2.1 | Tests unitarios para toda lógica nueva o modificada | **GATE** |
| 2.2 | Cobertura ≥ umbral configurado del repo, y **no baja** respecto de `master` | **GATE** |
| 2.3 | Pruebas de aceptación (Karate) si el cambio altera comportamiento observable por API | **GATE** (si aplica) |
| 2.4 | Pruebas de rendimiento (JMeter) si el cambio entra en los criterios de §3.4 | **GATE** (si aplica) |
| 2.5 | `architectureTest` (ArchUnit) / `arch:check` (dependency-cruiser) en verde | **GATE** |
| 2.6 | README actualizado si el cambio toca algo que documenta | **GATE** |
| 2.7 | Colección Postman actualizada si el repo la tiene y el cambio toca la API | **GATE** (si aplica) |
| 2.8 | Commits separados por tipo de cambio (fix real ≠ refactor estructural) | **GATE** |
| 2.9 | Verificación en vivo (no solo tests) para cambios de seguridad o concurrencia | **GATE** (si aplica) |
| — | Sin regresiones: la suite completa (`check` / `npm test`) pasa | **GATE** |
| — | Sin secretos en el diff (credenciales, tokens, claves) | **GATE** |

---

## 3. Detalle por categoría

### 3.1 Tests unitarios — **[GATE]**

**Qué exige.** Todo método/función con **lógica** (una rama, un cálculo, una
transformación, un mapeo no trivial, un guard) nuevo o modificado lleva test
unitario. Getters/setters, constructores triviales y DTOs sin comportamiento no.

**Qué cubrir**, como mínimo, por unidad tocada:
- el **camino feliz**;
- cada **rama** relevante (condiciones, `switch`, ternarios sustituidos por `if`);
- los **bordes** (colección vacía, `null`, un solo elemento, elemento `null` en
  una lista, tamaños desparejos entre dos listas paralelas);
- los **fallos** esperados (excepción de dominio, `Optional.empty`, timeout).

**Convención de mocks (Spock).** La **primera** interacción declarada sobre un
mock gana, no la última — un stub fijo en `setup()` bloquea uno más específico
declarado luego en el `given:` del test; usar un closure dinámico si el test
necesita afinarlo. Un `Mock()` de Spock devuelve `null` (no `Optional.empty()`)
para métodos sin stub.

**Adapters.** Un adapter de persistencia lleva test de **mapeo en ambos
sentidos** (dominio ⇄ documento/entidad) y de la **traducción de excepciones de
framework** que haga en su borde.

**Cómo verificar**: §6. **Qué bloquea**: cualquier lógica nueva sin test; un
test que "pasa" pero no afirma nada (`match` sin `assert`, `response.x` suelto).

### 3.2 Cobertura de código — **[GATE]**

**Qué exige.** La cobertura del repo **cumple el umbral configurado** y **no
baja** respecto de `master`. El umbral vive en el build del repo, **no en este
documento** (ver §8). Hoy: backends `jacocoTestCoverageVerification`
(típicamente BRANCH 0.85 / LINE 0.80); frontends `coverageThresholds` en
`angular.json` (típicamente 85% en las 4 métricas).

**Reglas duras.**
- **Prohibido bajar el umbral** para que pase un cambio. Si el cambio no puede
  alcanzarlo, el problema es la falta de tests, no el umbral.
- Código legítimamente no cubrible (POJOs Lombok, `main()`, config) va a la
  **lista de exclusión ya definida** del repo (`jacocoExcludes` / config), no se
  "tapa" con tests vacíos.
- Si tocás un archivo con cobertura por debajo del umbral **preexistente**, no
  estás obligado a subir todo el archivo, pero **tu código nuevo sí cumple** y
  no empeorás el número.

**Cómo verificar**: §6. **Qué bloquea**: `jacocoTestCoverageVerification` /
`ng test --coverage` en rojo; el número total baja respecto de `master`.

### 3.3 Pruebas de aceptación (Karate) — **[GATE si aplica]**

**Cuándo aplica.** El cambio **altera comportamiento observable por la API**:
- un endpoint nuevo, o un cambio de status/estructura/validación de uno existente;
- un campo nuevo en un request o response que un cliente puede ver;
- un cambio de autorización (un rol/empresa que antes podía y ahora no, o al revés);
- un cambio de reglas de negocio que cambia qué devuelve el endpoint.

**Cuándo NO aplica**: refactor interno sin cambio de contrato; cambio solo de
logs/métricas; cambio solo de tests o docs.

**Reglas.**
- La infra de aceptación corre contra **`_e2e-infra`** (docker local:
  mysql/mongo/redis/kafka). **NUNCA contra la RDS ni el Mongo compartidos**, ni
  QA, ni prod, ni dev remoto.
- Los seeds viven en `_e2e-infra/seed/<repo>.sql` y `<repo>.mongo.js`, son
  **idempotentes** (borran sus propios docs antes de insertar) y los aplica
  `run.sh` después del bootRun.
- El escenario **afirma el resultado real** (`status 201` + `match` del cuerpo),
  no `assert responseStatus != 500`. Un happy-path que solo verifica "no
  reventó" no es una prueba de aceptación.
- **Tags** (`@production-safe`, `@read-only`, `@full`, `@smoke`): significan
  cosas distintas repo a repo. Antes de tagear un escenario nuevo, comparalo
  contra un **hermano de escritura del mismo repo**, no contra otro repo.

**Cómo verificar**: §6. **Qué bloquea**: cambio de contrato sin escenario nuevo
o actualizado; la suite Karate en rojo.

### 3.4 Pruebas de rendimiento (JMeter) — **[GATE si aplica]**

**Cuándo aplica.** El cambio hace **alguna** de estas cosas:
- **(a)** agrega una query nueva a la BD, o un índice nuevo;
- **(b)** cambia la **selectividad o cardinalidad** de una query existente
  (nuevo filtro, `IN` que crece, join nuevo, `findFirst`/`OrderBy` agregado);
- **(c)** introduce un riesgo de **N+1** (query dentro de un loop / `stream`
  sobre resultados);
- **(d)** toca un **hot path**: endpoints de **listado/consulta**, el filtro de
  autenticación, jobs `@Scheduled`/batch, consumers Kafka, generación de
  documentos/PDF;
- **(e)** cambia **pool de conexiones**, ejecución **async**, **locking**
  (`@Version`, `SELECT ... FOR UPDATE`, ShedLock), o timeouts/circuit breakers.

**Cuándo NO aplica** (mayoría de los cambios):
- un campo nuevo en un documento/DTO **sin query nueva** (la data ya estaba en
  memoria);
- cambio de forma de la respuesta, de mensajes de error, de validación de entrada;
- cambios solo de docs, tests, logs.

**Qué exige cuando aplica.** Correr el plan del repo
(`external-test/jmeter/plans/<repo>-test-plan.jmx`) contra `_e2e-infra` **antes
y después** del cambio y comparar: throughput, p95/p99, tasa de error. El
cambio no puede degradar p95 de forma significativa (criterio de referencia:
> 20% sin justificación funcional). Adjuntar el resumen al PR.

**Zona gris.** Si dudás entre (a)–(e) y "no aplica", **declaralo explícitamente
en el PR**: qué query se agrega/cambia y por qué no degrada (ejemplo real: un
`findByCufe` que **reemplaza** condicionalmente a un `findByNumeroFactura`
preexistente → misma cantidad de queries por request, mismo perfil de costo →
no aplica JMeter, y se dejó escrito).

### 3.5 Arquitectura hexagonal — **[GATE]**

**Regla dura**: **ningún cambio se mergea con `architectureTest` /
`arch:check` en rojo.** Sin excepciones, sin "lo arreglo después".

**Reglas de dependencia (backend).**
| Capa | Solo puede depender de |
|------|------------------------|
| `domain` | nada del proyecto ni del framework (solo `java`/`jakarta` base). |
| `application` | `application`, `domain`, `java`/`javax`/`jakarta`, `io.swagger.v3.oas`, `com.factoring.commons`. **No Spring, no Spring Data, no Feign.** |
| `web` | `web`, `application`, `domain`, `org.springframework..`, swagger. **No `infrastructure`.** |
| `infrastructure` | todo lo anterior + el framework — **y traduce las excepciones de framework en su borde** (p. ej. `DuplicateKeyException` → excepción de dominio; nunca dejar salir `org.springframework.dao..` hacia `application`). |

**Dónde va lo nuevo.**
- Un **campo de dominio** nuevo → en el modelo de `domain` **y** en su
  entidad/documento de `infrastructure`, mapeado en el adapter en ambos
  sentidos. Anotaciones de framework (`@Field`, `@Column`) **solo** en la
  entidad de infraestructura, y solo si el repo ya las usa (si el resto del
  documento va sin `@Field`, el campo nuevo también).
- Una **query nueva** → firma en el puerto (`application`), implementación en el
  adapter (`infrastructure`).
- Una **excepción de negocio** nueva → en `application` (o `domain`); su
  `@ExceptionHandler` en `web`/`infrastructure`.

**Frontend.** `npm run arch:check` (dependency-cruiser): sin ciclos, sin
huérfanos, y las reglas de capa del repo. Los servicios se cablean por su
**clase abstracta como token de DI**, no por la implementación concreta.

Las reglas exactas de cada repo están codificadas en su `src/architecture-test/`
(backend) o `.dependency-cruiser.js` (frontend) — son la fuente de verdad.

### 3.6 Documentación (README) — **[GATE]**

**Qué exige.** Si el cambio toca algo que el README documenta, **el README
queda correcto en el mismo PR**.

- Un endpoint nuevo o con contrato cambiado → su sección (path, roles, request,
  **cuerpo de respuesta**, códigos de error).
- Un campo nuevo en un contrato → aparece en el ejemplo de request/response.
- Un modelo de datos / colección Mongo documentado → el campo nuevo, con su
  cardinalidad.
- Una env var nueva → la tabla de configuración.

**No se parchea, se deja completo.** Si al tocar una sección encontrás que
estaba desactualizada o incompleta (un endpoint sin schema de respuesta, un
campo stale), se **completa esa sección**, no se agrega solo la línea nueva
encima de lo viejo.

### 3.7 Colección de Postman — **[GATE si aplica]**

**Cuándo aplica.** El repo tiene una colección **versionada** (por convención,
`external-test/smoke/postman/<repo>.postman_collection.json`) **y** el cambio
toca la API.

**Qué exige.**
- Request nuevo → agregarlo.
- Request existente con contrato cambiado → actualizar body/params/headers.
- Si la colección **usa test scripts** (`pm.test(...)`), agregar/ajustar la
  aserción del campo tocado.
- Si la colección es un **catálogo de requests sin test scripts**, actualizar la
  `description` del request. **No introducir una convención de test que la
  colección no tiene.**
- El JSON queda **válido** (parsea).

### 3.8 Separación de commits — **[GATE]**

**Una rama `feature/...` por repo. Un _concern_ por commit.**

- El **fix real** de un bug va en su propio commit, **separado del refactor
  estructural** que lo habilita o lo acompaña. Si para arreglar el bug tuviste
  que mover código a la capa correcta, son dos commits.
- Prefijos: `feat:` · `fix(security):` · `fix(concurrencia):` ·
  `fix(robustez):` · `refactor:` · `test:` · `test(karate):` · `docs:`.
- Cada commit **compila y pasa sus propios tests**.
- **Diff revisado antes de comitear** cada commit significativo.
- **Preferir un commit nuevo** a enmendar uno existente.
- Cada acción irreversible (`push`, `merge`, `delete`) en **su propia
  invocación auditable**, nunca enterrada en un `&&` largo con otros pasos.
- Push sin abrir PR salvo que se pida explícitamente (respaldo remoto).

### 3.9 Verificación en vivo — **[GATE si aplica]**

**Obligatoria** para `fix(security)` y `fix(concurrencia)`. Recomendada para
cualquier cambio cuyo efecto no se ve claro solo en los tests.

**El patrón** (el que se aplicó toda esta sesión):
1. **Reproducir el estado malo** en `_e2e-infra` (sembrar la colisión, el
   recurso ajeno, las dos filas concurrentes).
2. **Antes**: correr el escenario contra el **código viejo** (o la rama base) y
   capturar el resultado incorrecto (500, valor equivocado, 200 donde debía ser
   403).
3. **Después**: mismo escenario contra el código nuevo, capturar el resultado
   correcto.
4. Incluir explícitamente el **escenario específico** del hallazgo: el vector de
   ataque (p. ej. actualizar + validar SFTP apuntando a un host interno), la
   condición de carrera exacta, la colisión de clave.
5. **Nunca** contra ambientes compartidos.

Adjuntar el antes/después al PR (o al mensaje del commit, como se viene
haciendo).

---

## 4. Ambientes y seguridad

- **`_e2e-infra/`** (`C:\WorkSpace\Factoring\_e2e-infra\`, no es repo git): stack
  docker-compose local (mysql 13306 / mongo 37017 / redis 16379 / kafka 19092).
  `./run.sh <repo>` levanta infra + bootRun + seed + Karate.
- **Regla absoluta**: **nada de escritura, ni de lectura exploratoria, contra la
  RDS compartida, el Mongo compartido, QA, prod, ni dev remoto** sin
  autorización explícita del dueño **en el momento**. Una aprobación para un
  ambiente no se extiende a otro ni a otra ocasión.
- **Scripts de índice / migración de esquema** (p. ej.
  `db/mongo-indexes/*.js`): se **versionan** pero **no se ejecutan** contra
  ningún ambiente que no sea la infra local sin coordinar la ejecución
  puntualmente.
- **Secretos**: nunca en un commit, un diff, un log, ni un argumento de
  herramienta. Credenciales en YAML son deuda conocida a migrar a Vault; no se
  agregan nuevas.

---

## 5. El gate de merge y la evidencia mínima

**Bloquean el merge (hard gates):** 2.1, 2.2, 2.5, 2.8, la suite completa en
verde, y — cuando aplican — 2.3, 2.4, 2.6, 2.7, 2.9.

**Evidencia mínima que el revisor debe ver antes de aprobar**, dado que el CI
no es confiable hoy:

| Situación del cambio | Evidencia exigida en el PR |
|----------------------|----------------------------|
| **Cualquier cambio** | Salida de `check` (backend) / `npm test` + `npm run arch:check` (frontend) en verde, con el **conteo de tests** y los **números de cobertura** (BRANCH/LINE o las 4 métricas). Lista de commits con su prefijo. |
| **Cambia contrato de API** | + salida de la suite Karate con el conteo de escenarios y el **escenario nuevo/actualizado** citado por nombre. |
| **`fix(security)` / `fix(concurrencia)`** | + **diff completo** del cambio + **verificación en vivo antes/después** siguiendo §3.9, con el escenario específico del hallazgo (vector de ataque / condición de carrera / colisión) mostrado bloqueado. |
| **Entra en §3.4** | + resumen JMeter antes/después (throughput, p95, error rate) **o** la declaración explícita de por qué no degrada. |
| **Toca README / Postman** | + confirmación de qué sección/request se actualizó. |

**Regla del revisor**: si un ítem que aplica no tiene su evidencia, **no se
aprueba** — se pide la evidencia, no se asume.

**Violaciones preexistentes**: si tocás un archivo con un problema previo
(checkstyle, cobertura baja, test de mala calidad, doc stale), arreglás lo que
está **en el alcance de tu cambio** y **anotás** en el PR lo que queda fuera —
no lo arreglás en silencio (ensucia el diff) ni lo ignorás (se pierde).

---

## 6. Backend ↔ Frontend — mapeo de herramientas

| Ítem DoD | Backend (Gradle / Java 17) | Frontend (npm / Angular) |
|----------|----------------------------|--------------------------|
| 2.1 Tests unitarios | Spock (`src/test/groovy`) — `./gradlew test` | Jasmine + Karma (`*.spec.ts`) — `npm test` |
| 2.2 Cobertura | JaCoCo — `./gradlew jacocoTestCoverageVerification`; umbral en `coverage.gradle` | `ng test --coverage`; umbral `coverageThresholds` en `angular.json` |
| 2.3 Aceptación / API | Karate (`src/automated-test`) — `./gradlew automatedTest -Pkarate.env=dev` contra `_e2e-infra` | Playwright (`e2e/`) — `npm run e2e` |
| 2.4 Rendimiento | JMeter — `jmeter -n -t external-test/jmeter/plans/<repo>-test-plan.jmx -q external-test/jmeter/config/<env>.properties` | (según app; normalmente N/A salvo Lighthouse/bundle-size si el repo lo define) |
| 2.5 Arquitectura | ArchUnit — `./gradlew architectureTest` (`src/architecture-test`) | dependency-cruiser — `npm run arch:check` (`.dependency-cruiser.js`) |
| — Estilo estático | checkstyle + PMD + SpotBugs (dentro de `check`) | ESLint — `npm run lint` |
| — Suite completa | `./gradlew check` (excluye `automatedTest`) | `npm test && npm run lint && npm run arch:check` |
| — Testcontainers | requiere Docker; si Docker Desktop ≥ 29 → `DOCKER_API_VERSION=1.51` (lo reenvía `test.gradle`) | N/A |

---

## 7. Caso de referencia — el fix de `cufes` en el endoso (2026-09-07)

Cambio cross-repo (`ms-negociador-orq` escribe, `ms-consultasFacturas-neg` lee):
agregar el CUFE de cada factura al endoso para resolver la factura exacta sin
depender del `numeroFactura` (consecutivo del emisor, no único global). Así se
ve un cambio que cumple la DoD:

| Ítem | Cómo se cumplió |
|------|-----------------|
| Diseño antes de codear | Se confirmó contra el dominio real la **cardinalidad** (`cufes` es lista, paralela a `numerosFactura`, porque un endoso grupal cubre varias facturas) y el **origen del dato** (la factura ya está en memoria al crear el endoso) **antes** de escribir una línea. |
| 2.1 Unit | +6 Spock en negociador (puebla cufes, orden paralelo en grupal, tolera factura sin cufe, round-trip del adapter), +7 en consultasFacturas (resuelve por cufe y no llama a numeroFactura, fallback, elemento null). |
| 2.2 Cobertura | negociador BRANCH 0.8557 / LINE 0.9018; consultasFacturas BRANCH 0.9386 / LINE 0.9824. Ninguno bajó. |
| 2.3 Karate | negociador **115/115** con un escenario que hace `POST .../endosos` **real → 201** y `match response.cufesEndosadas == [...]` (antes ese happy-path era "assert != 500"). consultasFacturas **198/198** con un escenario que verifica la **resolución por cufe ante colisión**. |
| 2.4 JMeter | **negociador**: no aplica (campo nuevo, dato en memoria, 0 queries nuevas) — declarado. **consultasFacturas**: `findByCufe` **reemplaza** condicionalmente a `findByNumeroFactura` → misma cantidad de queries, mismo costo, 0 índices nuevos — declarado, sin correr JMeter. |
| 2.5 ArchUnit | verde en ambos. `cufes` es `List<String>` puro en `domain`; el mapeo vive en `infrastructure`. Gotcha: **checkstyle prohíbe el ternario** → reescrito con `if`. |
| 2.6 README | negociador: bloque del endpoint + sección "Modelo de Datos". consultasFacturas: la sección de `/consulta/endosos` era **una fila de tabla sin schema** → se documentó el **cuerpo de respuesta completo**. |
| 2.7 Postman | negociador: test nuevo `cufesEndosadas paralelo a facturasEndosadas`. consultasFacturas: `description` del request (la colección no tiene test scripts). |
| 2.8 Commits | 3 por repo: `feat:` (código + unit) / `test(karate):` / `docs:`. Diff revisado antes de cada uno. Push en su propia invocación. |
| 2.9 Live | Se sembró la **colisión** (dos `Facturas` con `numeroFactura` "FE-CF-001", valores 1M y ~89M). Antes: la heurística devolvía **~89M** (el emisor equivocado) **sin ningún error**. Después: **1M**, la factura del cufe correcto. |
| Hallazgo colateral | El happy-path de creación de endoso **nunca se había verificado** en la infra e2e — faltaban la plantilla `ENDOSO_DOCUMENTO` en `messages_templates` y que `run.sh` aplicara seeds Mongo. Se arreglaron los dos. |

---

## 8. Apéndice — bloque de _specifics_ del repo

Cada `CONTRIBUTING.md` cierra con este bloque, completado con los valores reales
del repo:

```markdown
## Specifics de <nombre-repo>

- **Stack**: <Spring Boot X.Y / Angular X.Y>
- **Suite completa**: `<comando>`
- **Umbral de cobertura**: <BRANCH x / LINE y  |  statements/branches/functions/lines = z%>
- **Arquitectura**: `<architectureTest | arch:check>` — módulos/capas: <…>
- **Aceptación**: `<automatedTest -Pkarate.env=dev | e2e>` · infra: `_e2e-infra/repos/<repo>.env` · seeds: `<repo>.sql` `<repo>.mongo.js`
- **Rendimiento**: <plan JMeter `external-test/jmeter/plans/<repo>-test-plan.jmx` | N/A>
- **Postman**: <`external-test/smoke/postman/<repo>.postman_collection.json` (catálogo, sin test scripts | con test scripts) | no tiene>
- **Notas**: <p. ej. `SPRING_PROFILES_ACTIVE=dev` obligatorio; `DOCKER_API_VERSION=1.51`; puerto e2e distinto del default>
```

### Enlaces

- **`_e2e-infra`** — stack local, seeds y `run.sh`: `C:\WorkSpace\Factoring\_e2e-infra\` (`README.md` + `run.sh`).
- **Reglas de arquitectura de cada repo**: backend → `src/architecture-test/` (ArchUnit); frontend → `.dependency-cruiser.js`.
- **JMeter**: `external-test/jmeter/` de cada backend (`plans/`, `config/<ambiente>.properties`).

---

## 9. Repos nuevos — **[GATE del primer PR]**

Un repo nuevo (backend o frontend) **no se considera inicializado** hasta que
tiene su `CONTRIBUTING.md`:

- Con el **link a este documento** (`DEFINITION_OF_DONE.md` en `ms-commons-util`).
- Con el **bloque de _specifics_ (§8) completado** con los valores reales del
  repo — stack, comandos, umbral de cobertura configurado, rutas de seed /
  Postman / JMeter según lo que el repo **de verdad** tenga.

Va **en el primer commit** que crea el repo (o, para los repos que ya existen
sin él, en su propio commit `docs:` — nunca mezclado con otro trabajo).

**Por qué**: los repos sin estándar aplicado que aparecieron esta sesión no
tenían nadie que lo hubiera definido al arrancar. El costo de agregarlo el día 1
es cinco minutos; el de no hacerlo son meses de código sin checklist.

El revisor del primer PR de un repo nuevo **no aprueba** si falta el
`CONTRIBUTING.md` o si el bloque de _specifics_ tiene placeholders sin
completar.
