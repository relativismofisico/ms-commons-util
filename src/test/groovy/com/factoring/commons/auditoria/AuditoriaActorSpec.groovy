package com.factoring.commons.auditoria

import com.factoring.commons.mdc.MdcConstants
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import spock.lang.Specification

class AuditoriaActorSpec extends Specification {

    MockHttpServletRequest request = new MockHttpServletRequest()

    def cleanup() {
        SecurityContextHolder.clearContext()
    }

    private static void autenticar(Object principal, String rol) {
        SecurityContextHolder.context.authentication = new UsernamePasswordAuthenticationToken(
                principal, null, [new SimpleGrantedAuthority(rol)])
    }

    def "constructores explícitos para listeners y schedulers"() {
        expect:
        AuditoriaActor.usuario("op1", "OPERARIO", 7600L) == new AuditoriaActor("USUARIO", "op1", "OPERARIO", 7600L, null)
        AuditoriaActor.empresaSinUsuario(9701L) == new AuditoriaActor("EMPRESA_SIN_USUARIO", "empresa:9701", null, 9701L, null)
        AuditoriaActor.cuentaServicio("ms-liquidador-neg").tipo() == "CUENTA_SERVICIO"
        AuditoriaActor.sistema("ms-instrucciones-util:PaymentScheduler").id() == "ms-instrucciones-util:PaymentScheduler"
        AuditoriaActor.anonimo() == new AuditoriaActor("ANONIMO", null, null, null, null)
        AuditoriaActor.usuario("op1", "OPERARIO", 7600L).conEmpresaNit("900").empresaNit() == "900"
    }

    def "validación: tipo obligatorio, e id obligatorio salvo para ANONIMO"() {
        when:
        new AuditoriaActor(tipo, id, null, null, null)

        then:
        thrown(excepcion)

        where:
        tipo      | id   | excepcion
        null      | "x"  | NullPointerException
        "USUARIO" | null | IllegalArgumentException
        "SISTEMA" | " "  | IllegalArgumentException
    }

    def "desdeRequestActual: sin autenticación o anónima -> ANONIMO"() {
        given:
        if (anonima) {
            SecurityContextHolder.context.authentication = new AnonymousAuthenticationToken(
                    "k", "anonymousUser", [new SimpleGrantedAuthority("ROLE_ANONYMOUS")])
        }

        expect:
        AuditoriaActor.desdeRequestActual(request) == AuditoriaActor.anonimo()

        where:
        anonima << [false, true]
    }

    def "desdeRequestActual: usuario del JWT, su rol y la empresa del atributo (#empresa)"() {
        given:
        autenticar("KARATE-CF-PROV", "EMPRESA")
        request.setAttribute(MdcConstants.COMPANY_ID_ATTR, empresa)

        expect:
        AuditoriaActor.desdeRequestActual(request) == AuditoriaActor.usuario("KARATE-CF-PROV", "EMPRESA", esperado)

        where:
        empresa   | esperado
        9301L     | 9301L
        9301      | 9301L
        "9301"    | 9301L
        "no-num"  | null
        null      | null
    }

    def "desdeRequestActual: token de empresa sin usuario humano -> EMPRESA_SIN_USUARIO"() {
        given:
        autenticar(7500L, "REGISTRO_SESION")
        request.setAttribute(MdcConstants.COMPANY_ID_ATTR, 7500L)
        request.setAttribute(MdcConstants.SIN_USUARIO_HUMANO_ATTR, Boolean.TRUE)

        expect:
        AuditoriaActor.desdeRequestActual(request) == AuditoriaActor.empresaSinUsuario(7500L)
    }
}
