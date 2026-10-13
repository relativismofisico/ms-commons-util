package com.factoring.commons.mdc;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Pone en el MDC quién actúa, tomado del JWT YA VALIDADO, nunca de lo que el cliente declare en el body:
 * <ul>
 *   <li>{@link MdcConstants#USER}: el nombre de la {@link Authentication} del {@link SecurityContextHolder} (el
 *       {@code sub} del token; en llamadas entre microservicios, la cuenta de servicio). Vacío si la request es
 *       anónima, o si el filtro JWT marcó {@link MdcConstants#SIN_USUARIO_HUMANO_ATTR} (token de una empresa sin
 *       usuario humano, como la sesión temprana de registro).</li>
 *   <li>{@link MdcConstants#COMPANY_ID}: el atributo {@link MdcConstants#COMPANY_ID_ATTR}, que el filtro JWT del
 *       servicio llena con el claim {@code emp} después de validar el token. Vacío si no lo hay (cuentas de servicio,
 *       Basic auth, registro anónimo).</li>
 * </ul>
 *
 * <p>Tiene que correr DESPUÉS de la cadena de Spring Security: antes, el {@code SecurityContextHolder} todavía está
 * vacío. {@link MdcAutoConfiguration} lo registra con un orden explícito, inmediatamente después del de la cadena. Lo
 * que ocurre dentro de la cadena (rate limit, validación del token) solo lleva el requestId.</p>
 */
public class ActorMdcFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
            final FilterChain chain) throws ServletException, IOException {
        final String user = usuarioAutenticado();
        if (user != null && !Boolean.TRUE.equals(request.getAttribute(MdcConstants.SIN_USUARIO_HUMANO_ATTR))) {
            MDC.put(MdcConstants.USER, user);
        }
        final Object empresa = request.getAttribute(MdcConstants.COMPANY_ID_ATTR);
        if (empresa != null && MdcConstants.esSeguro(empresa.toString())) {
            MDC.put(MdcConstants.COMPANY_ID, empresa.toString());
        }
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MdcConstants.USER);
            MDC.remove(MdcConstants.COMPANY_ID);
        }
    }

    private static String usuarioAutenticado() {
        final Authentication autenticacion = SecurityContextHolder.getContext().getAuthentication();
        if (autenticacion == null || !autenticacion.isAuthenticated()
                || autenticacion instanceof AnonymousAuthenticationToken) {
            return null;
        }
        final String nombre = autenticacion.getName();
        if (MdcConstants.esSeguro(nombre)) {
            return nombre;
        }
        return null;
    }
}
