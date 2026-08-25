package com.factoring.commons.web;

import java.io.IOException;
import java.time.Duration;

import com.factoring.commons.util.IpUtils;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Límite de solicitudes por IP, respaldado por Redis (compartido entre todas
 * las instancias del servicio detrás del load balancer) — corre ANTES del
 * filtro JWT de cada backend, así rechaza tráfico abusivo sin gastar CPU en
 * validar token, y cubre todos los endpoints, no solo login.
 *
 * <p>Extraída a la librería compartida en la Fase 11 del pipeline de
 * mantenibilidad: el mecanismo (INCR en Redis + EXPIRE solo en el primer
 * hit de la ventana, mismo patrón de key {@code ratelimit:<servicio>:<ip>})
 * era idéntico en los 10 backends — lo único que variaba de verdad era el
 * formato del cuerpo del error 429, que queda inyectado vía
 * {@link RateLimitResponseWriter} en vez de fijo acá.
 */
@Slf4j
public class RateLimitFilter extends OncePerRequestFilter {

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;
    private final boolean enabled;
    private final long maxRequests;
    private final long windowSeconds;
    private final RateLimitResponseWriter responseWriter;

    /**
     * Construye el filtro con el template de Redis, los límites configurados
     * y la estrategia de escritura del error 429.
     *
     * @param redisTemplate  template de Redis para operaciones String
     * @param serviceName    nombre corto del servicio, usado como parte de la
     *                       key de Redis ({@code ratelimit:<serviceName>:<ip>})
     *                       — típicamente {@code spring.application.name}
     * @param enabled        si el límite está activo (permite desactivarlo por ambiente)
     * @param maxRequests    máximo de solicitudes permitidas por IP dentro de la ventana
     * @param windowSeconds  duración de la ventana, en segundos
     * @param responseWriter escribe el cuerpo de la respuesta 429 con el DTO
     *                       de error propio de cada backend consumidor
     */
    public RateLimitFilter(final StringRedisTemplate redisTemplate, final String serviceName,
            final boolean enabled, final long maxRequests, final long windowSeconds,
            final RateLimitResponseWriter responseWriter) {
        this.redisTemplate = redisTemplate;
        this.keyPrefix = "ratelimit:" + serviceName + ":";
        this.enabled = enabled;
        this.maxRequests = maxRequests;
        this.windowSeconds = windowSeconds;
        this.responseWriter = responseWriter;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
            final FilterChain filterChain) throws ServletException, IOException {
        if (!enabled) {
            filterChain.doFilter(request, response);
            return;
        }

        final String ip = IpUtils.obtenerIp(request);
        final String key = keyPrefix + ip;
        final Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1L) {
            redisTemplate.expire(key, Duration.ofSeconds(windowSeconds));
        }

        if (count != null && count > maxRequests) {
            log.warn("[RateLimitFilter] IP {} supero el limite de {} solicitudes en {}s en {}",
                    ip, maxRequests, windowSeconds, request.getRequestURI());
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setHeader("Retry-After", String.valueOf(windowSeconds));
            responseWriter.writeBody(request, response);
            return;
        }

        filterChain.doFilter(request, response);
    }
}
