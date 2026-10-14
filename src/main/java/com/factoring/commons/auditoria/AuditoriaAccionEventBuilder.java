package com.factoring.commons.auditoria;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.factoring.commons.mdc.MdcConstants;
import com.factoring.commons.util.IpUtils;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.MDC;

/**
 * Builder de {@link AuditoriaAccionEvent}. Valida los campos obligatorios y rechaza claves de {@code detalle} que
 * huelan a secreto: el log de auditoría es permanente y lo consultan personas, así que un OTP o un hash de contraseña
 * guardado ahí sería una fuga que no se puede borrar.
 */
public final class AuditoriaAccionEventBuilder {

    /** Fragmentos de clave prohibidos en {@code detalle} (comparación sin mayúsculas). */
    static final Set<String> FRAGMENTOS_PROHIBIDOS = Set.of(
            "otp", "password", "contrasena", "contraseña", "clave", "token", "hash", "secret", "secreto");

    private final String eventId = UUID.randomUUID().toString();
    private Instant occurredAt = Instant.now();
    private String servicioOrigen;
    private String accion;
    private AuditoriaAccionEvent.Entidad entidad;
    private AuditoriaActor actor;
    private String canal;
    private String ip;
    private String requestId = MDC.get(MdcConstants.REQUEST_ID);
    private String estadoAnterior;
    private String estadoNuevo;
    private final Map<String, Object> detalle = new LinkedHashMap<>();

    AuditoriaAccionEventBuilder() {
    }

    /**
     * @param valor cuándo ocurrió la acción (por defecto, ahora)
     * @return este builder
     */
    public AuditoriaAccionEventBuilder occurredAt(final Instant valor) {
        this.occurredAt = valor;
        return this;
    }

    /**
     * @param valor servicio de origen; si se omite, el publicador pone {@code spring.application.name}
     * @return este builder
     */
    public AuditoriaAccionEventBuilder servicioOrigen(final String valor) {
        this.servicioOrigen = valor;
        return this;
    }

    /**
     * @param valor acción; ver {@link Acciones}
     * @return este builder
     */
    public AuditoriaAccionEventBuilder accion(final String valor) {
        this.accion = valor;
        return this;
    }

    /**
     * @param tipo tipo de entidad; ver {@link EntidadTipo}
     * @param id   identificador de la entidad
     * @return este builder
     */
    public AuditoriaAccionEventBuilder entidad(final String tipo, final Object id) {
        this.entidad = new AuditoriaAccionEvent.Entidad(tipo, Objects.toString(id, null));
        return this;
    }

    /**
     * @param valor quién ejecutó la acción
     * @return este builder
     */
    public AuditoriaAccionEventBuilder actor(final AuditoriaActor valor) {
        this.actor = valor;
        return this;
    }

    /**
     * @param valor canal; ver {@link Canal}
     * @return este builder
     */
    public AuditoriaAccionEventBuilder canal(final String valor) {
        this.canal = valor;
        return this;
    }

    /**
     * @param valor IP del cliente tomada en el servidor
     * @return este builder
     */
    public AuditoriaAccionEventBuilder ip(final String valor) {
        this.ip = valor;
        return this;
    }

    /**
     * Toma la IP de la request ({@link IpUtils#obtenerIp}, primer valor de {@code X-Forwarded-For} o la conexión).
     *
     * @param request la request en curso
     * @return este builder
     */
    public AuditoriaAccionEventBuilder ipDe(final HttpServletRequest request) {
        this.ip = IpUtils.obtenerIp(request);
        return this;
    }

    /**
     * @param valor requestId de correlación; por defecto el del MDC del hilo que creó el builder
     * @return este builder
     */
    public AuditoriaAccionEventBuilder requestId(final String valor) {
        this.requestId = valor;
        return this;
    }

    /**
     * @param anterior estado antes de la acción
     * @param nuevo    estado después de la acción
     * @return este builder
     */
    public AuditoriaAccionEventBuilder estados(final Object anterior, final Object nuevo) {
        this.estadoAnterior = Objects.toString(anterior, null);
        this.estadoNuevo = Objects.toString(nuevo, null);
        return this;
    }

    /**
     * Agrega un dato propio de la acción. Un valor {@code null} se omite.
     *
     * @param clave nombre del dato; no puede parecer un secreto
     * @param valor valor serializable a JSON
     * @return este builder
     * @throws IllegalArgumentException si la clave contiene un fragmento prohibido (otp, password, token, hash…)
     */
    public AuditoriaAccionEventBuilder detalle(final String clave, final Object valor) {
        final String normalizada = clave.toLowerCase(Locale.ROOT);
        for (String fragmento : FRAGMENTOS_PROHIBIDOS) {
            if (normalizada.contains(fragmento)) {
                throw new IllegalArgumentException(
                        "detalle." + clave + " no se audita: parece un secreto (" + fragmento + ")");
            }
        }
        if (valor != null) {
            detalle.put(clave, valor);
        }
        return this;
    }

    /**
     * Construye el evento.
     *
     * @return el evento
     * @throws NullPointerException si falta accion, entidad (tipo e id), actor o canal
     */
    public AuditoriaAccionEvent build() {
        Objects.requireNonNull(accion, "accion es obligatoria");
        Objects.requireNonNull(entidad, "entidad es obligatoria");
        Objects.requireNonNull(entidad.tipo(), "entidad.tipo es obligatorio");
        Objects.requireNonNull(entidad.id(), "entidad.id es obligatorio");
        Objects.requireNonNull(actor, "actor es obligatorio");
        Objects.requireNonNull(canal, "canal es obligatorio");
        Objects.requireNonNull(occurredAt, "occurredAt es obligatorio");
        Map<String, Object> detalleFinal = null;
        if (!detalle.isEmpty()) {
            detalleFinal = detalle;
        }
        return new AuditoriaAccionEvent(eventId, AuditoriaAccionEvent.SCHEMA_VERSION_ACTUAL, occurredAt,
                servicioOrigen, accion, entidad, actor, canal, ip, requestId, estadoAnterior, estadoNuevo,
                detalleFinal);
    }
}
