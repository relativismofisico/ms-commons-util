package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import javax.sql.DataSource;

import com.factoring.commons.auditoria.AuditoriaAutoConfiguration;
import com.factoring.commons.auditoria.AuditoriaConstantes;
import com.factoring.commons.auditoria.AuditoriaPort;
import com.factoring.commons.auditoria.AuditoriaPublisher;
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter;
import com.factoring.commons.auditoria.outbox.mongo.MongoAuditoriaOutboxAdapter;
import net.javacrumbs.shedlock.core.LockProvider;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Outbox de auditoría ({@code auditoria.outbox.enabled=true}; por defecto apagado, y sin él no se registra ningún bean
 * de esta clase). Registra el almacén indicado en {@code auditoria.outbox.almacen} ({@code jdbc} o {@code mongo}, cada
 * uno solo si el servicio trae su librería), un {@link AuditoriaPort} que escribe en él, el relay, la limpieza y su
 * planificador.
 *
 * <p>No expone {@code JdbcTemplate}, {@code TransactionManager} ni {@code MongoTemplate} propios: usa los del servicio y
 * no compite por tipo con ellos.</p>
 */
@AutoConfiguration(after = AuditoriaAutoConfiguration.class, afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration"})
@ConditionalOnProperty(prefix = AuditoriaOutboxProperties.PREFIJO, name = "enabled", havingValue = "true")
@EnableConfigurationProperties(AuditoriaOutboxProperties.class)
public class AuditoriaOutboxAutoConfiguration {

    private static final String TOPIC = "${auditoria.topic:" + AuditoriaConstantes.TOPIC_POR_DEFECTO + "}";
    private static final String SERVICIO = "${spring.application.name:desconocido}";

    private final Clock reloj = Clock.systemUTC();

    /**
     * Puerto de auditoría respaldado por el outbox.
     *
     * @param propiedades    {@code auditoria.outbox.*} (se validan acá, antes de buscar el almacén)
     * @param outbox         almacén del outbox
     * @param topic          {@code auditoria.topic}
     * @param servicioOrigen {@code spring.application.name}
     * @return el puerto
     */
    @Bean
    @ConditionalOnMissingBean(AuditoriaPort.class)
    public OutboxAuditoriaPort outboxAuditoriaPort(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox, @Value(TOPIC) final String topic,
            @Value(SERVICIO) final String servicioOrigen) {
        propiedades.validar();
        return new OutboxAuditoriaPort(outbox.getObject(), topic, servicioOrigen, reloj);
    }

    /**
     * Relay del outbox.
     *
     * @param propiedades {@code auditoria.outbox.*}
     * @param outbox      almacén del outbox
     * @param publicador  publicador de auditoría ({@code auditoria.enabled} tiene que seguir activo)
     * @return el relay
     */
    @Bean
    public OutboxAuditoriaRelay outboxAuditoriaRelay(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox, final ObjectProvider<AuditoriaPublisher> publicador) {
        propiedades.validar();
        return new OutboxAuditoriaRelay(outbox.getObject(), publicador.getObject(), propiedades, reloj);
    }

    /**
     * Limpieza de los eventos enviados.
     *
     * @param propiedades    {@code auditoria.outbox.*}
     * @param outbox         almacén del outbox
     * @param bloqueo        bloqueo distribuido, si el servicio usa ShedLock
     * @param servicioOrigen {@code spring.application.name}
     * @return la limpieza
     */
    @Bean
    public AuditoriaOutboxLimpieza auditoriaOutboxLimpieza(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox, final ObjectProvider<BloqueoLimpieza> bloqueo,
            @Value(SERVICIO) final String servicioOrigen) {
        propiedades.validar();
        return new AuditoriaOutboxLimpieza(outbox.getObject(), propiedades,
                bloqueo.getIfAvailable(() -> BloqueoLimpieza.SIN_BLOQUEO), servicioOrigen, reloj);
    }

    /**
     * Planificador del relay y de la limpieza.
     *
     * @param relay       relay del outbox
     * @param limpieza    limpieza del outbox
     * @param propiedades {@code auditoria.outbox.*}
     * @return el planificador
     */
    @Bean
    public AuditoriaOutboxPlanificador auditoriaOutboxPlanificador(final OutboxAuditoriaRelay relay,
            final AuditoriaOutboxLimpieza limpieza, final AuditoriaOutboxProperties propiedades) {
        return new AuditoriaOutboxPlanificador(relay, limpieza, propiedades);
    }

    /** Almacén JDBC: tabla {@code auditoria_outbox} en el DataSource del servicio. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(JdbcTemplate.class)
    @ConditionalOnProperty(prefix = AuditoriaOutboxProperties.PREFIJO, name = "almacen", havingValue = "jdbc")
    static class Jdbc {

        @Bean
        @ConditionalOnMissingBean(AuditoriaOutboxPort.class)
        JdbcAuditoriaOutboxAdapter jdbcAuditoriaOutboxAdapter(final DataSource dataSource) {
            return new JdbcAuditoriaOutboxAdapter(dataSource);
        }
    }

    /** Almacén Mongo: colección propia del servicio. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MongoOperations.class)
    @ConditionalOnProperty(prefix = AuditoriaOutboxProperties.PREFIJO, name = "almacen", havingValue = "mongo")
    static class Mongo {

        @Bean(initMethod = "crearIndices")
        @ConditionalOnMissingBean(AuditoriaOutboxPort.class)
        MongoAuditoriaOutboxAdapter mongoAuditoriaOutboxAdapter(final MongoOperations mongo,
                final AuditoriaOutboxProperties propiedades, @Value(SERVICIO) final String servicioOrigen) {
            return new MongoAuditoriaOutboxAdapter(mongo, coleccion(propiedades, servicioOrigen));
        }

        /** La configurada, o {@code auditoria_outbox_<servicio>} con el nombre normalizado a {@code [A-Za-z0-9_]}. */
        static String coleccion(final AuditoriaOutboxProperties propiedades, final String servicioOrigen) {
            final String configurada = propiedades.getMongo().getColeccion();
            if (configurada != null && !configurada.isBlank()) {
                return configurada;
            }
            return "auditoria_outbox_" + servicioOrigen.replaceAll("[^A-Za-z0-9_]", "_");
        }
    }

    /** Bloqueo de la limpieza con el {@code LockProvider} de ShedLock del servicio, si lo tiene. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(LockProvider.class)
    @ConditionalOnBean(LockProvider.class)
    static class ConShedLock {

        @Bean
        BloqueoLimpieza auditoriaOutboxBloqueoLimpieza(final LockProvider lockProvider,
                final AuditoriaOutboxProperties propiedades) {
            return new ShedLockBloqueoLimpieza(lockProvider, propiedades.getIntervaloLimpieza(), Clock.systemUTC());
        }
    }
}
