package com.factoring.commons.mdc

import jakarta.servlet.FilterChain
import org.slf4j.MDC
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import spock.lang.Specification

class ActorMdcFilterSpec extends Specification {

    ActorMdcFilter filter = new ActorMdcFilter()
    MockHttpServletRequest request = new MockHttpServletRequest()

    def cleanup() {
        SecurityContextHolder.clearContext()
        MDC.clear()
    }

    private static void autenticado(String nombre) {
        SecurityContextHolder.context.authentication = new UsernamePasswordAuthenticationToken(
                nombre, null, [new SimpleGrantedAuthority("EMPRESA")])
    }

    /** Ejecuta el filtro y devuelve [user, companyId] del MDC dentro de la cadena. */
    private List<String> mdcDentroDeLaCadena() {
        List<String> visto = null
        filter.doFilter(request, new MockHttpServletResponse(),
                { req, res -> visto = [MDC.get("user"), MDC.get("companyId")] } as FilterChain)
        visto
    }

    def "toma el usuario del SecurityContext (JWT ya validado) y la empresa del atributo que deja el filtro JWT"() {
        given:
        autenticado("KARATE-CF-PROV")
        request.setAttribute(MdcConstants.COMPANY_ID_ATTR, 9301L)

        expect:
        mdcDentroDeLaCadena() == ["KARATE-CF-PROV", "9301"]
    }

    def "cuenta de servicio sin claim emp: usuario sí, empresa vacía"() {
        given:
        autenticado("svc-liquidador")

        expect:
        mdcDentroDeLaCadena() == ["svc-liquidador", null]
    }

    def "token de una empresa sin usuario humano (sesión temprana/onboarding): usuario vacío, empresa del atributo"() {
        given: "el principal es el id de la empresa, como en ms-registroempresa-neg"
        SecurityContextHolder.context.authentication = new UsernamePasswordAuthenticationToken(
                7500L, null, [new SimpleGrantedAuthority("REGISTRO_SESION")])
        request.setAttribute(MdcConstants.SIN_USUARIO_HUMANO_ATTR, Boolean.TRUE)
        request.setAttribute(MdcConstants.COMPANY_ID_ATTR, 7500L)

        expect:
        mdcDentroDeLaCadena() == [null, "7500"]
    }

    def "request anónima o sin autenticación: sin usuario"() {
        given:
        if (anonima) {
            SecurityContextHolder.context.authentication = new AnonymousAuthenticationToken(
                    "clave", "anonymousUser", [new SimpleGrantedAuthority("ROLE_ANONYMOUS")])
        }

        expect:
        mdcDentroDeLaCadena() == [null, null]

        where:
        anonima << [true, false]
    }

    def "un valor con caracteres que permitirían inyectar líneas en el log no entra al MDC"() {
        given:
        autenticado("usuario\nFALSO")
        request.setAttribute(MdcConstants.COMPANY_ID_ATTR, "1 2")

        expect:
        mdcDentroDeLaCadena() == [null, null]
    }

    def "al terminar quita sus claves del MDC y deja el requestId, que es del RequestIdFilter"() {
        given:
        autenticado("u1")
        request.setAttribute(MdcConstants.COMPANY_ID_ATTR, "7")
        MDC.put("requestId", "r1")

        when:
        filter.doFilter(request, new MockHttpServletResponse(), { req, res -> } as FilterChain)

        then:
        MDC.get("user") == null
        MDC.get("companyId") == null
        MDC.get("requestId") == "r1"
    }
}
