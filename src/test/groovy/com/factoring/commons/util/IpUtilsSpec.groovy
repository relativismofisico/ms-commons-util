package com.factoring.commons.util

import jakarta.servlet.http.HttpServletRequest
import spock.lang.Specification

class IpUtilsSpec extends Specification {

    HttpServletRequest request = Mock()

    def "obtenerIp usa el primer valor de X-Forwarded-For cuando está presente"() {
        given:
        request.getHeader("X-Forwarded-For") >> "198.51.100.7, 10.0.0.1"

        expect:
        IpUtils.obtenerIp(request) == "198.51.100.7"
    }

    def "obtenerIp cae a X-Real-IP cuando X-Forwarded-For está ausente"() {
        given:
        request.getHeader("X-Forwarded-For") >> null
        request.getHeader("X-Real-IP") >> "198.51.100.9"

        expect:
        IpUtils.obtenerIp(request) == "198.51.100.9"
    }

    def "obtenerIp ignora headers con valor 'unknown' o vacío"() {
        given:
        request.getHeader("X-Forwarded-For") >> forwardedFor
        request.getHeader("X-Real-IP") >> realIp
        request.getRemoteAddr() >> "203.0.113.5"

        expect:
        IpUtils.obtenerIp(request) == "203.0.113.5"

        where:
        forwardedFor | realIp
        "unknown"    | null
        ""           | null
        null         | "unknown"
        null         | ""
    }

    def "obtenerIp cae a remoteAddr cuando no hay ningún header de proxy"() {
        given:
        request.getHeader("X-Forwarded-For") >> null
        request.getHeader("X-Real-IP") >> null
        request.getRemoteAddr() >> "203.0.113.5"

        expect:
        IpUtils.obtenerIp(request) == "203.0.113.5"
    }
}
