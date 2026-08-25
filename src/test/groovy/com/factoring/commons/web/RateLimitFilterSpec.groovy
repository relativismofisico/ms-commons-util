package com.factoring.commons.web

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations
import spock.lang.Specification

import java.time.Duration

class RateLimitFilterSpec extends Specification {

    private static final long MAX_REQUESTS = 10L
    private static final long WINDOW_SECONDS = 60L
    // 203.0.113.0/24 es el rango TEST-NET-3 reservado por RFC 5737 para documentación/pruebas
    private static final String IP = "203.0.113.10"

    StringRedisTemplate redisTemplate = Mock()
    ValueOperations<String, String> valueOperations = Mock()
    RateLimitResponseWriter responseWriter = Mock()
    HttpServletRequest request = Mock()
    HttpServletResponse response = Mock()
    FilterChain filterChain = Mock()

    RateLimitFilter filter

    def setup() {
        filter = new RateLimitFilter(redisTemplate, "test-service", true, MAX_REQUESTS, WINDOW_SECONDS, responseWriter)
        request.getRemoteAddr() >> IP
        redisTemplate.opsForValue() >> valueOperations
    }

    def "deshabilitado continúa la cadena sin tocar Redis"() {
        given:
        def disabledFilter = new RateLimitFilter(redisTemplate, "test-service", false, MAX_REQUESTS, WINDOW_SECONDS, responseWriter)

        when:
        disabledFilter.doFilterInternal(request, response, filterChain)

        then:
        1 * filterChain.doFilter(request, response)
        0 * redisTemplate._
    }

    def "dentro del límite continúa la cadena"() {
        given:
        request.getHeader("X-Forwarded-For") >> null
        valueOperations.increment(_ as String) >> 5L

        when:
        filter.doFilterInternal(request, response, filterChain)

        then:
        1 * filterChain.doFilter(request, response)
        0 * response.setStatus(429)
        0 * responseWriter.writeBody(_, _)
    }

    def "primera solicitud de la ventana setea el TTL"() {
        given:
        request.getHeader("X-Forwarded-For") >> null
        valueOperations.increment(_ as String) >> 1L

        when:
        filter.doFilterInternal(request, response, filterChain)

        then:
        1 * redisTemplate.expire("ratelimit:test-service:" + IP, Duration.ofSeconds(WINDOW_SECONDS))
    }

    def "solicitudes siguientes dentro de la ventana no vuelven a setear el TTL"() {
        given:
        request.getHeader("X-Forwarded-For") >> null
        valueOperations.increment(_ as String) >> 2L

        when:
        filter.doFilterInternal(request, response, filterChain)

        then:
        0 * redisTemplate.expire(_, _)
    }

    def "supera el límite: responde 429, delega el cuerpo al writer, no continúa la cadena"() {
        given:
        request.getHeader("X-Forwarded-For") >> null
        valueOperations.increment(_ as String) >> (MAX_REQUESTS + 1)

        when:
        filter.doFilterInternal(request, response, filterChain)

        then:
        0 * filterChain.doFilter(_, _)
        1 * response.setStatus(429)
        1 * response.setHeader("Retry-After", String.valueOf(WINDOW_SECONDS))
        1 * responseWriter.writeBody(request, response)
    }

    def "usa la IP de X-Forwarded-For cuando está presente, no la del socket"() {
        given:
        request.getHeader("X-Forwarded-For") >> "198.51.100.7, 10.0.0.1"
        valueOperations.increment(_ as String) >> 1L

        when:
        filter.doFilterInternal(request, response, filterChain)

        then:
        1 * valueOperations.increment("ratelimit:test-service:198.51.100.7")
    }
}
