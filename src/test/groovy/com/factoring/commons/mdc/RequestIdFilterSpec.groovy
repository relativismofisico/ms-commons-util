package com.factoring.commons.mdc

import jakarta.servlet.DispatcherType
import jakarta.servlet.FilterChain
import org.slf4j.MDC
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import spock.lang.Specification

class RequestIdFilterSpec extends Specification {

    RequestIdFilter filter = new RequestIdFilter()
    MockHttpServletResponse response = new MockHttpServletResponse()

    def cleanup() {
        MDC.clear()
    }

    /** Ejecuta el filtro y devuelve el requestId que había en el MDC dentro de la cadena. */
    private String requestIdDentroDeLaCadena(MockHttpServletRequest request) {
        String visto = null
        filter.doFilter(request, response, { req, res -> visto = MDC.get("requestId") } as FilterChain)
        visto
    }

    def "sin header genera un requestId nuevo, lo pone en el MDC y lo devuelve en la respuesta"() {
        when:
        String visto = requestIdDentroDeLaCadena(new MockHttpServletRequest())

        then:
        visto ==~ /[0-9a-f-]{36}/
        response.getHeader("X-Request-Id") == visto
    }

    def "reusa el requestId recibido si es seguro: la correlación sigue entre microservicios"() {
        given:
        def request = new MockHttpServletRequest()
        request.addHeader("X-Request-Id", "req-123_abc.9")

        expect:
        requestIdDentroDeLaCadena(request) == "req-123_abc.9"
        response.getHeader("X-Request-Id") == "req-123_abc.9"
    }

    def "descarta un header inseguro y genera otro: '#recibido'"() {
        given:
        def request = new MockHttpServletRequest()
        request.addHeader("X-Request-Id", recibido)

        when:
        String visto = requestIdDentroDeLaCadena(request)

        then:
        visto != recibido
        visto ==~ /[0-9a-f-]{36}/

        where:
        recibido << ["abc\nFALSO [INFO] linea inyectada", "con espacio", "x" * 65, ""]
    }

    def "al terminar limpia las tres claves del MDC, también si la cadena lanza una excepción"() {
        given:
        def request = new MockHttpServletRequest()

        when:
        filter.doFilter(request, response, { req, res ->
            MDC.put("user", "u1")
            MDC.put("companyId", "7")
            throw new IllegalStateException("falla")
        } as FilterChain)

        then:
        thrown(IllegalStateException)
        MDC.get("requestId") == null
        MDC.get("user") == null
        MDC.get("companyId") == null
    }

    def "en el dispatch de error reusa el requestId de la misma request"() {
        given:
        def request = new MockHttpServletRequest()
        String original = requestIdDentroDeLaCadena(request)
        request.setDispatcherType(DispatcherType.ERROR)
        request.removeAttribute(RequestIdFilter.name + ".FILTERED")

        expect:
        requestIdDentroDeLaCadena(request) == original
    }
}
