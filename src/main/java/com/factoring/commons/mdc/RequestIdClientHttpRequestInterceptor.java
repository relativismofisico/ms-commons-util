package com.factoring.commons.mdc;

import java.io.IOException;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

/**
 * Propaga el requestId en las llamadas salientes de {@code RestTemplate} y {@code RestClient} (los dos aceptan un
 * {@link ClientHttpRequestInterceptor}).
 *
 * <p>No se registra solo: cada servicio lo agrega a mano SOLO en los clientes que llaman a otro microservicio propio,
 * nunca en los de terceros (captcha, Carvajal/Siigo, ipify), para no enviarles el identificador interno:</p>
 * <pre>
 * restTemplate.getInterceptors().add(new RequestIdClientHttpRequestInterceptor());
 * RestClient.builder().requestInterceptor(new RequestIdClientHttpRequestInterceptor())
 * </pre>
 */
public class RequestIdClientHttpRequestInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(final HttpRequest request, final byte[] body,
            final ClientHttpRequestExecution execution) throws IOException {
        final String requestId = RequestIdPropagation.actual();
        if (requestId != null && !request.getHeaders().containsKey(MdcConstants.REQUEST_ID_HEADER)) {
            request.getHeaders().set(MdcConstants.REQUEST_ID_HEADER, requestId);
        }
        return execution.execute(request, body);
    }
}
