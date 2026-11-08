package com.factoring.commons.outbox;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Formato JSON del sobre {@link EventoOutbox}, igual en productores y consumidores e independiente del
 * {@code ObjectMapper} de cada servicio: fechas ISO-8601, sin headers de tipo y campos desconocidos tolerados.
 */
public final class OutboxJson {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private OutboxJson() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Una copia del mapper con este formato, para que el consumidor arme su deserializer.
     *
     * @return copia del mapper
     */
    public static ObjectMapper mapper() {
        return MAPPER.copy();
    }

    /**
     * Serializa el sobre.
     *
     * @param evento sobre a serializar
     * @return el JSON
     * @throws IllegalStateException si el payload no se puede serializar
     */
    public static String serializar(final EventoOutbox<?> evento) {
        try {
            return MAPPER.writeValueAsString(evento);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el evento " + evento.eventId() + " ("
                    + evento.tipo() + ")", e);
        }
    }

    /**
     * Lee un sobre con su payload del tipo indicado.
     *
     * @param json         el JSON
     * @param clasePayload clase del payload
     * @param <T>          tipo del payload
     * @return el sobre
     * @throws IllegalArgumentException si el JSON no es un sobre válido
     */
    public static <T> EventoOutbox<T> deserializar(final String json, final Class<T> clasePayload) {
        final JavaType tipo = MAPPER.getTypeFactory().constructParametricType(EventoOutbox.class, clasePayload);
        try {
            return MAPPER.readValue(json, tipo);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Evento de outbox inválido: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * Las cabeceras Kafka del sobre, leídas del JSON guardado (sin deserializar el payload).
     *
     * @param json el sobre serializado
     * @return cabeceras {@code eventId}, {@code tipo}, {@code version} y {@code servicioOrigen}, en ese orden
     * @throws IllegalArgumentException si el JSON no es un sobre o le falta {@code eventId} o {@code tipo}
     */
    public static Map<String, String> cabeceras(final String json) {
        final JsonNode nodo;
        try {
            nodo = MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Evento de outbox ilegible", e);
        }
        if (nodo == null || !nodo.hasNonNull("eventId") || !nodo.hasNonNull("tipo")) {
            throw new IllegalArgumentException("Evento de outbox sin eventId o tipo");
        }
        final Map<String, String> cabeceras = new LinkedHashMap<>();
        cabeceras.put(EventoOutbox.CABECERA_EVENT_ID, nodo.get("eventId").asText());
        cabeceras.put(EventoOutbox.CABECERA_TIPO, nodo.get("tipo").asText());
        cabeceras.put(EventoOutbox.CABECERA_VERSION, nodo.path("version").asText());
        cabeceras.put(EventoOutbox.CABECERA_SERVICIO_ORIGEN, nodo.path("servicioOrigen").asText());
        return cabeceras;
    }
}
