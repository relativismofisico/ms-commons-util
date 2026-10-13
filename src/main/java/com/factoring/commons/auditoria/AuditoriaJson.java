package com.factoring.commons.auditoria;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Formato JSON del evento de auditoría, igual en productores y consumidor e independiente del {@code ObjectMapper} que
 * cada servicio configure: fechas ISO-8601, sin headers de tipo, y campos desconocidos tolerados (un productor más
 * nuevo puede agregar campos sin romper a un consumidor más viejo).
 */
public final class AuditoriaJson {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private AuditoriaJson() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Una copia del mapper con este formato, para que el consumidor arme su deserializer Kafka. Copia y no la
     * instancia compartida: quien la reciba puede reconfigurarla sin cambiar el formato de los demás.
     *
     * @return copia del mapper
     */
    public static ObjectMapper mapper() {
        return MAPPER.copy();
    }

    /**
     * Lee un evento.
     *
     * @param json el JSON
     * @return el evento
     * @throws IllegalArgumentException si el JSON no es un evento válido (campos obligatorios, actor inválido)
     */
    public static AuditoriaAccionEvent deserializar(final String json) {
        try {
            return MAPPER.readValue(json, AuditoriaAccionEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Evento de auditoría inválido: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * Serializa el evento.
     *
     * @param evento evento a serializar
     * @return el JSON
     * @throws IllegalStateException si el evento no se puede serializar (un {@code detalle} con un valor no
     *                               serializable)
     */
    public static String serializar(final AuditoriaAccionEvent evento) {
        try {
            return MAPPER.writeValueAsString(evento);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el evento de auditoría " + evento.eventId(), e);
        }
    }
}
