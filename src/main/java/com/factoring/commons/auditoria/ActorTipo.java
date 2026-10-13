package com.factoring.commons.auditoria;

/** Catálogo de tipos de actor de {@link AuditoriaActor}. */
public final class ActorTipo {

    /** Usuario humano autenticado por JWT. */
    public static final String USUARIO = "USUARIO";
    /** Empresa con token propio y sin usuario humano (sesión temprana y onboarding del registro). */
    public static final String EMPRESA_SIN_USUARIO = "EMPRESA_SIN_USUARIO";
    /** Otro microservicio de la plataforma con su cuenta de servicio. */
    public static final String CUENTA_SERVICIO = "CUENTA_SERVICIO";
    /** Proceso automático del propio servicio (scheduler o consumidor de un evento sin actor humano). */
    public static final String SISTEMA = "SISTEMA";
    /** Sin autenticación. */
    public static final String ANONIMO = "ANONIMO";

    private ActorTipo() {
        throw new IllegalStateException("Utility class");
    }
}
