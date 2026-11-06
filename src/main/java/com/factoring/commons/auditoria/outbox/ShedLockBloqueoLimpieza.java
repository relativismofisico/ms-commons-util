package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import java.time.Duration;

import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;

/** {@link BloqueoLimpieza} sobre el {@link LockProvider} de ShedLock que ya tenga el servicio. */
public class ShedLockBloqueoLimpieza implements BloqueoLimpieza {

    private final LockingTaskExecutor ejecutor;
    private final Duration maximo;
    private final Clock reloj;

    /**
     * @param lockProvider proveedor de bloqueos del servicio
     * @param maximo       tiempo máximo que se retiene el bloqueo si la instancia cae a medias
     * @param reloj        reloj de la fecha de creación del bloqueo
     */
    public ShedLockBloqueoLimpieza(final LockProvider lockProvider, final Duration maximo, final Clock reloj) {
        this.ejecutor = new DefaultLockingTaskExecutor(lockProvider);
        this.maximo = maximo;
        this.reloj = reloj;
    }

    @Override
    public void ejecutar(final String nombre, final Runnable tarea) {
        ejecutor.executeWithLock(tarea, new LockConfiguration(reloj.instant(), nombre, maximo, Duration.ZERO));
    }
}
