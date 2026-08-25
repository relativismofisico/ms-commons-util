package com.factoring.commons.web;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Escribe el cuerpo de la respuesta 429 cuando {@link RateLimitFilter} corta
 * una solicitud por exceso de tasa.
 *
 * <p>El core del rate-limiting (contador en Redis, TTL de ventana, status
 * code, header {@code Retry-After}) es idéntico en los 10 backends de la
 * plataforma y vive en {@link RateLimitFilter}. Lo que SÍ varía por repo es
 * el formato del cuerpo del error — cada uno serializa su propio DTO de
 * error (que todavía no está 100% unificado en toda la plataforma) con su
 * propio mecanismo (algunos usan {@code ObjectMapper} directo, otros
 * {@code MappingJackson2HttpMessageConverter}), y alguno incluye el
 * {@code path} de la request en el body (ej. {@code ApiError} de
 * {@code ms-postliquidacion-util}) — por eso el writer recibe también el
 * {@link HttpServletRequest}, no solo la response. Esa parte queda
 * inyectada en vez de fija en la librería compartida.
 */
@FunctionalInterface
public interface RateLimitResponseWriter {

    /**
     * Escribe el cuerpo (content-type incluido) de la respuesta 429. El
     * status code y el header {@code Retry-After} ya están seteados por
     * {@link RateLimitFilter} antes de invocar este método.
     *
     * @param request  solicitud original, por si el DTO de error del repo
     *                 incluye datos derivados de ella (ej. el path)
     * @param response respuesta HTTP, ya con status 429 y Retry-After seteados
     * @throws IOException si falla la escritura del cuerpo
     */
    void writeBody(HttpServletRequest request, HttpServletResponse response) throws IOException;
}
