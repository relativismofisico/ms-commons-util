package com.factoring.commons.mdc

import jakarta.servlet.Filter
import jakarta.servlet.FilterChain
import jakarta.servlet.ServletRequest
import jakarta.servlet.ServletResponse
import org.slf4j.MDC
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.context.runner.WebApplicationContextRunner
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.mock.web.MockFilterChain
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import spock.lang.Specification

import jakarta.servlet.http.HttpServlet
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse

class MdcAutoConfigurationSpec extends Specification {

    WebApplicationContextRunner contexto = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MdcAutoConfiguration))

    def cleanup() {
        SecurityContextHolder.clearContext()
        MDC.clear()
    }

    def "registra RequestIdFilter primero y ActorMdcFilter justo después de la cadena de Spring Security (-100 + 1)"() {
        expect:
        contexto.run { ctx ->
            assert ctx.getBean("requestIdFilterRegistration", FilterRegistrationBean).order == Integer.MIN_VALUE
            assert ctx.getBean("actorMdcFilterRegistration", FilterRegistrationBean).order == -99
            assert ctx.getBeansOfType(RequestIdFeignInterceptor).size() == 1
        }
    }

    def "si el servicio cambia spring.security.filter.order, el filtro de actor se mueve con la cadena"() {
        expect:
        contexto.withPropertyValues("spring.security.filter.order=10").run { ctx ->
            assert ctx.getBean("actorMdcFilterRegistration", FilterRegistrationBean).order == 11
        }
    }

    def "sin Spring Security no registra el filtro de actor; sin Feign, no registra el interceptor de Feign"() {
        expect:
        contexto.withClassLoader(new FilteredClassLoader(SecurityContextHolder, feign.RequestInterceptor)).run { ctx ->
            assert ctx.containsBean("requestIdFilterRegistration")
            assert !ctx.containsBean("actorMdcFilterRegistration")
            assert ctx.getBeansOfType(RequestIdFeignInterceptor).isEmpty()
        }
    }

    def "fuera de una aplicación web servlet no registra nada"() {
        expect:
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(MdcAutoConfiguration)).run { ctx ->
            assert !ctx.containsBean("requestIdFilterRegistration")
        }
    }

    def "con los órdenes registrados, el filtro de actor ve el SecurityContext que llenó la cadena de seguridad"() {
        given: "una 'cadena de seguridad' en -100 que autentica, como el filtro JWT de cada servicio"
        Filter cadenaSeguridad = { ServletRequest req, ServletResponse res, FilterChain chain ->
            SecurityContextHolder.context.authentication = new UsernamePasswordAuthenticationToken("KARATE-CF-PROV", null, [])
            req.setAttribute(MdcConstants.COMPANY_ID_ATTR, 9301L)
            try {
                chain.doFilter(req, res)
            } finally {
                SecurityContextHolder.clearContext()
            }
        } as Filter
        Map vistoEnLogica = null
        HttpServlet logica = new HttpServlet() {
            @Override
            protected void service(HttpServletRequest req, HttpServletResponse res) {
                vistoEnLogica = [requestId: MDC.get("requestId"), user: MDC.get("user"), companyId: MDC.get("companyId")]
            }
        }

        and: "los filtros ordenados como los ordena el contenedor"
        List<Filter> filtros = null
        contexto.run { ctx ->
            def registros = [[orden: ctx.getBean("requestIdFilterRegistration", FilterRegistrationBean).order,
                              filtro: ctx.getBean("requestIdFilterRegistration", FilterRegistrationBean).filter],
                             [orden: -100, filtro: cadenaSeguridad],
                             [orden: ctx.getBean("actorMdcFilterRegistration", FilterRegistrationBean).order,
                              filtro: ctx.getBean("actorMdcFilterRegistration", FilterRegistrationBean).filter]]
            filtros = registros.sort { it.orden }*.filtro
        }

        when:
        new MockFilterChain(logica, filtros as Filter[]).doFilter(new MockHttpServletRequest(), new MockHttpServletResponse())

        then:
        vistoEnLogica.requestId != null
        vistoEnLogica.user == "KARATE-CF-PROV"
        vistoEnLogica.companyId == "9301"
        MDC.copyOfContextMap == null || MDC.copyOfContextMap.isEmpty()
    }
}
