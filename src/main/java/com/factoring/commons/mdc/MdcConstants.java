package com.factoring.commons.mdc;

import java.util.regex.Pattern;

/**
 * Nombres compartidos de la trazabilidad por request: claves del MDC, header de correlación y el atributo de request
 * por el que cada filtro JWT entrega la empresa del token.
 *
 * <p>El patrón de log de cada servicio ({@code logging.pattern.console}) referencia las claves del MDC por estos
 * nombres: {@code %X{requestId}}, {@code %X{user}}, {@code %X{companyId}}.</p>
 */
public final class MdcConstants {

    /** Clave MDC del identificador de correlación de la request (propio o recibido por {@link #REQUEST_ID_HEADER}). */
    public static final String REQUEST_ID = "requestId";

    /**
     * Clave MDC del actor autenticado: el {@code sub} del JWT ya validado (usuario, o cuenta de servicio en llamadas
     * entre microservicios). Nunca un dato declarado por el cliente en el body.
     */
    public static final String USER = "user";

    /** Clave MDC de la empresa del actor: el claim {@code emp} del JWT ya validado. Vacía si el token no lo trae. */
    public static final String COMPANY_ID = "companyId";

    /** Header HTTP con el que viaja el requestId entre microservicios y vuelve en la respuesta. */
    public static final String REQUEST_ID_HEADER = "X-Request-Id";

    /**
     * Atributo de request donde el filtro JWT de cada servicio deja el claim {@code emp}, después de validar el token.
     * {@link ActorMdcFilter} lo lee de aquí: la librería no parsea tokens ni conoce los 10 filtros JWT.
     */
    public static final String COMPANY_ID_ATTR = "com.factoring.commons.mdc.companyId";

    /**
     * Atributo de request ({@code Boolean.TRUE}) con el que el filtro JWT indica que la autenticación no es de un usuario
     * humano aunque tenga principal: por ejemplo, los tokens de sesión temprana y de onboarding de ms-registroempresa-neg,
     * cuyo principal es el id de la empresa. {@link ActorMdcFilter} deja entonces {@link #USER} vacío; la empresa va en
     * {@link #COMPANY_ID_ATTR}.
     */
    public static final String SIN_USUARIO_HUMANO_ATTR = "com.factoring.commons.mdc.sinUsuarioHumano";

    /** Atributo de request con el requestId ya resuelto, para reusarlo si la misma request vuelve a pasar (dispatch de error). */
    static final String REQUEST_ID_ATTR = "com.factoring.commons.mdc.requestId";

    /**
     * Valores aceptados en el MDC desde fuera (header de correlación): sin espacios ni saltos de línea, que permitirían
     * inyectar líneas falsas en el log. Lo que no cumple se descarta (y en el caso del requestId se genera uno nuevo).
     */
    static final Pattern VALOR_SEGURO = Pattern.compile("^[A-Za-z0-9._:@+-]{1,64}$");

    private MdcConstants() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Indica si un valor externo puede ir al MDC tal cual.
     *
     * @param valor valor recibido
     * @return {@code true} si no es nulo y solo tiene caracteres seguros, con 64 como máximo
     */
    static boolean esSeguro(final String valor) {
        return valor != null && VALOR_SEGURO.matcher(valor).matches();
    }
}
