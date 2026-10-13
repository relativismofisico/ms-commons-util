package com.factoring.commons.mdc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * Trazabilidad por request, activa en cualquier servicio que dependa de la librería (sin configuración propia).
 *
 * <p>Orden explícito de los dos filtros respecto de la cadena de Spring Security, que Spring Boot registra como un
 * único filtro de servlet ({@code DelegatingFilterProxy}) con el orden {@code spring.security.filter.order} (-100 por
 * defecto):</p>
 * <ol>
 *   <li>{@link RequestIdFilter} en {@link #ORDEN_REQUEST_ID}: antes que todo, para que hasta el rate limit y el rechazo
 *       de un token lleven requestId;</li>
 *   <li>la cadena de Spring Security (filtro JWT del servicio, que llena el {@code SecurityContextHolder} y el atributo
 *       {@link MdcConstants#COMPANY_ID_ATTR});</li>
 *   <li>{@link ActorMdcFilter} en el orden de la cadena + 1: inmediatamente después, cuando el actor ya está resuelto.
 *       Se calcula desde la misma propiedad, así que sigue siendo correcto si un servicio la cambia.</li>
 * </ol>
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class MdcAutoConfiguration {

    /** Antes de todos los filtros de la aplicación, incluidos los de Spring Boot (encoding, etc.). */
    static final int ORDEN_REQUEST_ID = Ordered.HIGHEST_PRECEDENCE;

    /** Mismo valor por defecto que {@code SecurityProperties.DEFAULT_FILTER_ORDER} de Spring Boot. */
    static final int ORDEN_CADENA_SEGURIDAD_POR_DEFECTO = -100;

    /**
     * Registra {@link RequestIdFilter} como primer filtro de servlet.
     *
     * @return el registro del filtro
     */
    @Bean
    @ConditionalOnMissingBean(name = "requestIdFilterRegistration")
    public FilterRegistrationBean<RequestIdFilter> requestIdFilterRegistration() {
        final FilterRegistrationBean<RequestIdFilter> registro = new FilterRegistrationBean<>(new RequestIdFilter());
        registro.setName("requestIdFilter");
        registro.setOrder(ORDEN_REQUEST_ID);
        return registro;
    }

    /** Solo si el servicio usa Spring Security: sin él no hay actor que leer. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.security.core.context.SecurityContextHolder")
    static class ActorConfiguration {

        /**
         * Registra {@link ActorMdcFilter} inmediatamente después de la cadena de Spring Security.
         *
         * @param ordenCadenaSeguridad orden de la cadena ({@code spring.security.filter.order})
         * @return el registro del filtro
         */
        @Bean
        @ConditionalOnMissingBean(name = "actorMdcFilterRegistration")
        public FilterRegistrationBean<ActorMdcFilter> actorMdcFilterRegistration(
                @Value("${spring.security.filter.order:" + ORDEN_CADENA_SEGURIDAD_POR_DEFECTO + "}")
                final int ordenCadenaSeguridad) {
            final FilterRegistrationBean<ActorMdcFilter> registro = new FilterRegistrationBean<>(new ActorMdcFilter());
            registro.setName("actorMdcFilter");
            registro.setOrder(ordenCadenaSeguridad + 1);
            return registro;
        }
    }

    /** Solo si el servicio usa Feign: Spring Cloud OpenFeign aplica a todos sus clientes los {@code RequestInterceptor} del contexto. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "feign.RequestInterceptor")
    static class FeignConfiguration {

        /**
         * Interceptor que propaga el requestId en las llamadas Feign.
         *
         * @return el interceptor
         */
        @Bean
        @ConditionalOnMissingBean
        public RequestIdFeignInterceptor requestIdFeignInterceptor() {
            return new RequestIdFeignInterceptor();
        }
    }
}
