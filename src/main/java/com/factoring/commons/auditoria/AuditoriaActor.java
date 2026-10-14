package com.factoring.commons.auditoria;

import java.util.Objects;

import com.factoring.commons.mdc.MdcConstants;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Quién ejecutó una acción auditada.
 *
 * <p>En hilos de request se arma con {@link #desdeRequestActual(HttpServletRequest)}, desde la autenticación YA
 * validada y los atributos que deja el filtro JWT (mismo origen que el MDC), nunca desde lo que el cliente declare en
 * el body. En listeners de Kafka y schedulers no hay {@code SecurityContext}: el productor usa los constructores
 * explícitos ({@link #sistema}, {@link #usuario}, {@link #cuentaServicio}…) con lo que trae el evento o el registro
 * que disparó la acción.</p>
 *
 * @param tipo       tipo de actor; ver {@link ActorTipo}
 * @param id         sub del JWT, cuenta de servicio o identificador del proceso; {@code null} si es anónimo
 * @param rol        rol del token, si lo hay
 * @param empresaId  {@code company.ide} de la empresa actuante, si se conoce
 * @param empresaNit NIT de la empresa actuante, si se conoce
 */
public record AuditoriaActor(String tipo, String id, String rol, Long empresaId, String empresaNit) {

    /** Valida lo mínimo: el tipo siempre, y el id salvo para un actor anónimo. */
    public AuditoriaActor {
        Objects.requireNonNull(tipo, "actor.tipo es obligatorio");
        if (!ActorTipo.ANONIMO.equals(tipo) && (id == null || id.isBlank())) {
            throw new IllegalArgumentException("actor.id es obligatorio para un actor " + tipo);
        }
    }

    /**
     * Usuario humano autenticado.
     *
     * @param id         sub del JWT
     * @param rol        rol del token
     * @param empresaId  empresa del usuario (claim {@code emp}), si la tiene
     * @return el actor
     */
    public static AuditoriaActor usuario(final String id, final String rol, final Long empresaId) {
        return new AuditoriaActor(ActorTipo.USUARIO, id, rol, empresaId, null);
    }

    /**
     * Empresa que actúa sin un usuario humano: tokens de sesión temprana y de onboarding del registro.
     *
     * @param empresaId id de la empresa del token
     * @return el actor
     */
    public static AuditoriaActor empresaSinUsuario(final Long empresaId) {
        Objects.requireNonNull(empresaId, "empresaId es obligatorio para EMPRESA_SIN_USUARIO");
        return new AuditoriaActor(ActorTipo.EMPRESA_SIN_USUARIO, "empresa:" + empresaId, null, empresaId, null);
    }

    /**
     * Otro microservicio de la plataforma que llama con su cuenta de servicio.
     *
     * @param cuenta nombre de la cuenta de servicio
     * @return el actor
     */
    public static AuditoriaActor cuentaServicio(final String cuenta) {
        return new AuditoriaActor(ActorTipo.CUENTA_SERVICIO, cuenta, null, null, null);
    }

    /**
     * Proceso automático del propio servicio (scheduler, consumidor de un evento sin actor humano).
     *
     * @param proceso identificador del proceso, por ejemplo {@code ms-instrucciones-util:PaymentScheduler}
     * @return el actor
     */
    public static AuditoriaActor sistema(final String proceso) {
        return new AuditoriaActor(ActorTipo.SISTEMA, proceso, null, null, null);
    }

    /**
     * Sin autenticación (el {@code POST /company} del registro, antes de cualquier token).
     *
     * @return el actor
     */
    public static AuditoriaActor anonimo() {
        return new AuditoriaActor(ActorTipo.ANONIMO, null, null, null, null);
    }

    /**
     * El mismo actor con el NIT de su empresa, cuando el productor lo conoce.
     *
     * @param nit NIT de la empresa actuante
     * @return copia con el NIT
     */
    public AuditoriaActor conEmpresaNit(final String nit) {
        return new AuditoriaActor(tipo, id, rol, empresaId, nit);
    }

    /**
     * Actor de la request en curso, desde la autenticación ya validada y los atributos del filtro JWT:
     * <ul>
     *   <li>sin autenticación (o anónima): {@link #anonimo()};</li>
     *   <li>con {@link MdcConstants#SIN_USUARIO_HUMANO_ATTR}: {@link #empresaSinUsuario} con la empresa de
     *       {@link MdcConstants#COMPANY_ID_ATTR};</li>
     *   <li>en otro caso: {@link #usuario} con el nombre de la autenticación, su primera authority y la empresa de
     *       {@link MdcConstants#COMPANY_ID_ATTR} si la hay.</li>
     * </ul>
     * Una cuenta de servicio autenticada por Basic Auth sale como USUARIO: la librería no puede distinguirla en
     * general; el productor que la reconozca usa {@link #cuentaServicio}.
     *
     * @param request la request en curso
     * @return el actor
     */
    public static AuditoriaActor desdeRequestActual(final HttpServletRequest request) {
        final Authentication autenticacion = SecurityContextHolder.getContext().getAuthentication();
        if (autenticacion == null || !autenticacion.isAuthenticated()
                || autenticacion instanceof AnonymousAuthenticationToken) {
            return anonimo();
        }
        final Long empresaId = comoLong(request.getAttribute(MdcConstants.COMPANY_ID_ATTR));
        if (Boolean.TRUE.equals(request.getAttribute(MdcConstants.SIN_USUARIO_HUMANO_ATTR)) && empresaId != null) {
            return empresaSinUsuario(empresaId);
        }
        return usuario(autenticacion.getName(), primeraAuthority(autenticacion), empresaId);
    }

    private static String primeraAuthority(final Authentication autenticacion) {
        return autenticacion.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .findFirst()
                .orElse(null);
    }

    private static Long comoLong(final Object valor) {
        if (valor instanceof Number numero) {
            return numero.longValue();
        }
        if (valor instanceof String texto && !texto.isBlank()) {
            try {
                return Long.valueOf(texto.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
