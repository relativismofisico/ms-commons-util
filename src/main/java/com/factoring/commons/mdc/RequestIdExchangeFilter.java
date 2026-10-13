package com.factoring.commons.mdc;

import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;

/**
 * Propaga el requestId en las llamadas salientes de {@code WebClient}.
 *
 * <p>El requestId se lee del MDC al CREAR el filtro, no al ejecutar la llamada: un {@code WebClient} puede
 * suscribirse en un hilo de Reactor, donde el MDC de la request no existe. Por eso se crea uno por llamada, en el hilo
 * de la request, y solo en clientes hacia microservicios propios (hoy los únicos {@code WebClient} de la plataforma
 * son los de Carvajal/Siigo, terceros, que no lo llevan):</p>
 * <pre>
 * webClient.mutate().filter(RequestIdExchangeFilter.desdeRequestActual()).build()
 * </pre>
 */
public final class RequestIdExchangeFilter {

    private RequestIdExchangeFilter() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Filtro que agrega el requestId de la request en curso.
     *
     * @return el filtro; sin efecto si no hay requestId (fuera de una request)
     */
    public static ExchangeFilterFunction desdeRequestActual() {
        final String requestId = RequestIdPropagation.actual();
        return (request, next) -> {
            if (requestId == null || request.headers().containsKey(MdcConstants.REQUEST_ID_HEADER)) {
                return next.exchange(request);
            }
            return next.exchange(ClientRequest.from(request)
                    .header(MdcConstants.REQUEST_ID_HEADER, requestId)
                    .build());
        };
    }
}
