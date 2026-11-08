package com.factoring.commons.auditoria.outbox;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Almacén del outbox. Hay una implementación JDBC (tabla configurable, {@code auditoria_outbox} por defecto) y una
 * Mongo (colección propia del servicio); la autoconfiguración registra la que indique {@code auditoria.outbox.almacen}.
 * Guarda tanto los eventos de auditoría como los eventos genéricos de {@code com.factoring.commons.outbox}.
 *
 * <p>Las operaciones de marcado son condicionales: solo cambian el evento si sigue reclamado con el mismo lease
 * ({@code leaseHasta}) que devolvió {@link #tomarLote}. Un relay cuyo lease venció no pisa lo que hizo otro.</p>
 */
public interface AuditoriaOutboxPort {

    /**
     * Guarda el evento participando en la transacción activa del almacén (JDBC o Mongo); sin transacción, se escribe
     * y queda confirmado en el momento. Si falla, la excepción se propaga: el caso de uso no debe confirmar sin su
     * evento.
     *
     * @param evento evento nuevo ({@link AuditoriaOutboxEvento#nuevo})
     */
    void guardar(AuditoriaOutboxEvento evento);

    /**
     * Reclama hasta {@code tamano} eventos {@link EstadoOutbox#PENDIENTE} con {@code proximoIntento <= ahora}, en
     * orden de creación, y los deja {@link EstadoOutbox#EN_PROCESO} hasta {@code leaseHasta}. Solo toma un evento si
     * todos los anteriores no enviados de su misma clave Kafka también están en el lote. Dos relays a la vez nunca
     * reciben el mismo evento.
     *
     * @param tamano     máximo de eventos
     * @param ahora      instante de referencia
     * @param leaseHasta fin del lease
     * @return los eventos reclamados, en orden
     */
    List<AuditoriaOutboxEvento> tomarLote(int tamano, Instant ahora, Instant leaseHasta);

    /**
     * Eventos {@link EstadoOutbox#EN_PROCESO} cuyo lease venció (el relay que los tenía cayó a medias).
     *
     * @param ahora  instante de referencia
     * @param limite máximo de eventos
     * @return los eventos, con el lease vencido con el que se marcan después
     */
    List<AuditoriaOutboxEvento> buscarLeasesVencidos(Instant ahora, int limite);

    /**
     * Marca el evento como {@link EstadoOutbox#ENVIADO}. No exige el lease: Kafka ya lo confirmó.
     *
     * @param id         id del evento
     * @param fechaEnvio fecha de la confirmación
     * @return si lo marcó (falso si ya estaba enviado)
     */
    boolean marcarEnviado(String id, Instant fechaEnvio);

    /**
     * Devuelve el evento a {@link EstadoOutbox#PENDIENTE} para reintentarlo.
     *
     * @param evento         evento tal como se reclamó
     * @param intentos       intentos fallidos, ya contando este
     * @param proximoIntento desde cuándo se puede volver a tomar
     * @param error          causa saneada
     * @return si lo marcó (falso si ya no tenía ese lease)
     */
    boolean marcarReintento(AuditoriaOutboxEvento evento, int intentos, Instant proximoIntento, String error);

    /**
     * Marca el evento como {@link EstadoOutbox#FALLIDO}.
     *
     * @param evento   evento tal como se reclamó
     * @param intentos intentos fallidos, ya contando este
     * @param error    causa saneada
     * @return si lo marcó (falso si ya no tenía ese lease)
     */
    boolean marcarFallido(AuditoriaOutboxEvento evento, int intentos, String error);

    /**
     * Devuelve el evento a {@link EstadoOutbox#PENDIENTE} sin contar un intento (el relay no llegó a publicarlo).
     *
     * @param evento evento tal como se reclamó
     * @return si lo liberó (falso si ya no tenía ese lease)
     */
    boolean liberar(AuditoriaOutboxEvento evento);

    /**
     * Cuántos eventos esperan publicarse, cuántos fallaron y desde cuándo espera el más antiguo (métricas).
     *
     * @return el resumen
     */
    ResumenOutbox resumen();

    /**
     * Borra eventos {@link EstadoOutbox#ENVIADO} confirmados antes de {@code limite}.
     *
     * @param limite fecha de envío límite
     * @param maximo máximo de eventos a borrar en esta llamada
     * @return eventos borrados
     */
    int limpiarEnviadosAnteriores(Instant limite, int maximo);

    /**
     * El resumen separado por tipo de evento ({@link AuditoriaOutboxEvento#tipoMetricas()}). Por defecto, todo cuenta
     * como auditoría.
     *
     * @return resumen por tipo; un tipo sin eventos por enviar puede faltar
     */
    default Map<String, ResumenTipo> resumenPorTipo() {
        return Map.of(AuditoriaOutboxEvento.TIPO_AUDITORIA, ResumenTipo.de(resumen()));
    }

    /**
     * Comprueba al arrancar que el almacén puede guardar eventos genéricos (con {@code tipo}). Por defecto no exige
     * nada.
     *
     * @throws IllegalStateException si el esquema del almacén no admite eventos genéricos
     */
    default void verificarEventosGenericos() {
        // Sin requisitos por defecto.
    }
}
