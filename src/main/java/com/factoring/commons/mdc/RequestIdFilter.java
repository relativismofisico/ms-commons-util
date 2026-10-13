package com.factoring.commons.mdc;

import java.io.IOException;
import java.util.UUID;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Primer filtro de la request: fija el {@link MdcConstants#REQUEST_ID} en el MDC (el que llega en
 * {@link MdcConstants#REQUEST_ID_HEADER} si es seguro, o uno nuevo) y lo devuelve en la respuesta. Corre ANTES de la
 * cadena de Spring Security, para que también lleven el requestId los logs del rate limit y de un token rechazado.
 *
 * <p>Es el dueño del ciclo de vida del MDC: al terminar la request limpia las tres claves, incluidas las que
 * {@link ActorMdcFilter} agregó más adentro, para que el hilo del pool no herede el actor de la request anterior.</p>
 */
public class RequestIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
            final FilterChain chain) throws ServletException, IOException {
        final String requestId = resolver(request);
        request.setAttribute(MdcConstants.REQUEST_ID_ATTR, requestId);
        response.setHeader(MdcConstants.REQUEST_ID_HEADER, requestId);
        MDC.put(MdcConstants.REQUEST_ID, requestId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MdcConstants.REQUEST_ID);
            MDC.remove(MdcConstants.USER);
            MDC.remove(MdcConstants.COMPANY_ID);
        }
    }

    /** También en el dispatch de error (/error), reusando el mismo requestId: el log del error queda correlacionado. */
    @Override
    protected boolean shouldNotFilterErrorDispatch() {
        return false;
    }

    private static String resolver(final HttpServletRequest request) {
        final Object yaResuelto = request.getAttribute(MdcConstants.REQUEST_ID_ATTR);
        if (yaResuelto instanceof String existente) {
            return existente;
        }
        final String recibido = request.getHeader(MdcConstants.REQUEST_ID_HEADER);
        if (MdcConstants.esSeguro(recibido)) {
            return recibido;
        }
        return UUID.randomUUID().toString();
    }
}
