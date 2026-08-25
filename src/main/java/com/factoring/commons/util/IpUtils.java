package com.factoring.commons.util;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Resuelve la IP real del cliente detrás de un load balancer (AWS ALB u
 * otro proxy). Extraída a la librería compartida en la Fase 11 del pipeline
 * de mantenibilidad — era prácticamente idéntica en los 10 backends de la
 * plataforma, sin ninguna diferencia real de comportamiento entre repos.
 */
public final class IpUtils {

    private IpUtils() {
    }

    /**
     * Devuelve la IP real del cliente.
     *
     * @param request petición HTTP entrante
     * @return IP del cliente, resuelta desde {@code X-Forwarded-For},
     *         {@code X-Real-IP}, o como último recurso
     *         {@link HttpServletRequest#getRemoteAddr()}
     */
    public static String obtenerIp(final HttpServletRequest request) {
        String header = request.getHeader("X-Forwarded-For");

        if (header != null && !header.isEmpty() && !"unknown".equalsIgnoreCase(header)) {
            return header.split(",")[0].trim();
        }

        header = request.getHeader("X-Real-IP");

        if (header != null && !header.isEmpty() && !"unknown".equalsIgnoreCase(header)) {
            return header;
        }

        return request.getRemoteAddr();
    }
}
