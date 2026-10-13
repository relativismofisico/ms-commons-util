package com.factoring.commons.mdc;

import org.slf4j.MDC;

/**
 * Lectura del requestId de la request en curso, para propagarlo en {@link MdcConstants#REQUEST_ID_HEADER} a otro
 * microservicio propio. Lo usan los tres interceptores de salida ({@link RequestIdClientHttpRequestInterceptor},
 * {@link RequestIdFeignInterceptor}, {@link RequestIdExchangeFilter}).
 */
public final class RequestIdPropagation {

    private RequestIdPropagation() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * El requestId del MDC del hilo actual.
     *
     * @return el requestId, o {@code null} fuera de una request (scheduler, consumidor Kafka)
     */
    public static String actual() {
        return MDC.get(MdcConstants.REQUEST_ID);
    }
}
