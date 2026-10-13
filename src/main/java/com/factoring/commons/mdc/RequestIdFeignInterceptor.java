package com.factoring.commons.mdc;

import feign.RequestInterceptor;
import feign.RequestTemplate;

/**
 * Propaga el requestId en las llamadas Feign. {@link MdcAutoConfiguration} lo registra como bean cuando el servicio
 * trae Feign, y Spring Cloud OpenFeign lo aplica a todos los {@code @FeignClient} del servicio: hoy el único es el de
 * ms-liquidador-neg hacia ms-negociador-orq, un servicio propio. Un {@code @FeignClient} futuro hacia un tercero tendría
 * que excluirse con su propia configuración.
 */
public class RequestIdFeignInterceptor implements RequestInterceptor {

    @Override
    public void apply(final RequestTemplate template) {
        final String requestId = RequestIdPropagation.actual();
        if (requestId != null && !template.headers().containsKey(MdcConstants.REQUEST_ID_HEADER)) {
            template.header(MdcConstants.REQUEST_ID_HEADER, requestId);
        }
    }
}
