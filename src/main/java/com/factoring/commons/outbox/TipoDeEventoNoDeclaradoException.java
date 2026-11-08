package com.factoring.commons.outbox;

/** Se intentó registrar en el outbox un tipo de evento que el servicio no declaró en {@link TiposDeEvento}. */
public class TipoDeEventoNoDeclaradoException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    /**
     * @param tipo nombre del tipo no declarado
     */
    public TipoDeEventoNoDeclaradoException(final String tipo) {
        super("Tipo de evento no declarado en TiposDeEvento: " + tipo);
    }
}
