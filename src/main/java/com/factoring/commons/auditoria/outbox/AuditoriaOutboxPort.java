package com.factoring.commons.auditoria.outbox;

import java.time.Instant;
import java.util.List;

/**
 * Almacén del outbox de auditoría. Hay una implementación JDBC (tabla {@code auditoria_outbox}) y una Mongo (colección
 * propia del servicio); la autoconfiguración registra la que indique {@code auditoria.outbox.almacen}.
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
     * Borra eventos {@link EstadoOutbox#ENVIADO} confirmados antes de {@code limite}.
     *
     * @param limite fecha de envío límite
     * @param maximo máximo de eventos a borrar en esta llamada
     * @return eventos borrados
     */
    int limpiarEnviadosAnteriores(Instant limite, int maximo);
}
