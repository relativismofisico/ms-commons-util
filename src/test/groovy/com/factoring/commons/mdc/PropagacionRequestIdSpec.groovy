package com.factoring.commons.mdc

import feign.RequestTemplate
import org.slf4j.MDC
import org.springframework.http.HttpMethod
import org.springframework.http.client.ClientHttpRequestExecution
import org.springframework.mock.http.client.MockClientHttpRequest
import org.springframework.mock.http.client.MockClientHttpResponse
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import reactor.core.publisher.Mono
import spock.lang.Specification

class PropagacionRequestIdSpec extends Specification {

    def cleanup() {
        MDC.clear()
    }

    // ── RestTemplate / RestClient ──

    def "RestTemplate/RestClient: agrega X-Request-Id con el requestId del MDC"() {
        given:
        MDC.put("requestId", "r-1")
        def request = new MockClientHttpRequest(HttpMethod.GET, URI.create("http://negociador/x"))
        ClientHttpRequestExecution execution = Mock()

        when:
        new RequestIdClientHttpRequestInterceptor().intercept(request, new byte[0], execution)

        then:
        1 * execution.execute(request, _) >> new MockClientHttpResponse(new byte[0], 200)
        request.headers.getFirst("X-Request-Id") == "r-1"
    }

    def "RestTemplate/RestClient: sin requestId (fuera de una request) o con header ya puesto, no lo toca"() {
        given:
        if (enMdc) {
            MDC.put("requestId", "r-1")
        }
        def request = new MockClientHttpRequest(HttpMethod.GET, URI.create("http://negociador/x"))
        if (yaPuesto) {
            request.headers.set("X-Request-Id", "propio")
        }
        ClientHttpRequestExecution execution = Mock()

        when:
        new RequestIdClientHttpRequestInterceptor().intercept(request, new byte[0], execution)

        then:
        1 * execution.execute(request, _) >> new MockClientHttpResponse(new byte[0], 200)
        request.headers.get("X-Request-Id") == esperado

        where:
        enMdc | yaPuesto | esperado
        false | false    | null
        true  | true     | ["propio"]
    }

    // ── Feign ──

    def "Feign: agrega X-Request-Id con el requestId del MDC; sin requestId no agrega nada"() {
        given:
        if (requestId) {
            MDC.put("requestId", requestId)
        }
        def template = new RequestTemplate()

        when:
        new RequestIdFeignInterceptor().apply(template)

        then:
        template.headers().get("X-Request-Id") == esperado

        where:
        requestId | esperado
        "r-2"     | ["r-2"]
        null      | null
    }

    def "Feign: si el header ya viene no lo duplica"() {
        given:
        MDC.put("requestId", "r-2")
        def template = new RequestTemplate().header("X-Request-Id", "propio")

        when:
        new RequestIdFeignInterceptor().apply(template)

        then:
        template.headers().get("X-Request-Id") == ["propio"]
    }

    // ── WebClient ──

    def "WebClient: el requestId se captura al crear el filtro, aunque la llamada corra en otro hilo sin MDC"() {
        given:
        MDC.put("requestId", "r-3")
        def filtro = RequestIdExchangeFilter.desdeRequestActual()
        MDC.clear()
        ClientRequest enviada = null
        ExchangeFunction next = { ClientRequest req -> enviada = req; Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).build()) }

        when:
        filtro.filter(ClientRequest.create(HttpMethod.GET, URI.create("http://propio/x")).build(), next).block()

        then:
        enviada.headers().getFirst("X-Request-Id") == "r-3"
    }

    def "WebClient: sin requestId al crear el filtro, la request sale sin el header"() {
        given:
        def filtro = RequestIdExchangeFilter.desdeRequestActual()
        ClientRequest enviada = null
        ExchangeFunction next = { ClientRequest req -> enviada = req; Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).build()) }

        when:
        filtro.filter(ClientRequest.create(HttpMethod.GET, URI.create("http://propio/x")).build(), next).block()

        then:
        !enviada.headers().containsKey("X-Request-Id")
    }
}
