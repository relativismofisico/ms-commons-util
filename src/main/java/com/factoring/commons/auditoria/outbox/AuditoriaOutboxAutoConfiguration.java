package com.factoring.commons.auditoria.outbox;

import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;
import javax.sql.DataSource;

import com.factoring.commons.auditoria.AuditoriaAutoConfiguration;
import com.factoring.commons.auditoria.AuditoriaConstantes;
import com.factoring.commons.auditoria.AuditoriaPort;
import com.factoring.commons.auditoria.AuditoriaPublisher;
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter;
import com.factoring.commons.auditoria.outbox.mongo.MongoAuditoriaOutboxAdapter;
import com.factoring.commons.outbox.KafkaPublicadorOutbox;
import com.factoring.commons.outbox.OutboxPort;
import com.factoring.commons.outbox.OutboxRegistro;
import com.factoring.commons.outbox.PublicadorOutbox;
import com.factoring.commons.outbox.TiposDeEvento;
import io.micrometer.core.instrument.binder.MeterBinder;
import net.javacrumbs.shedlock.core.LockProvider;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Outbox de auditoría ({@code auditoria.outbox.enabled=true}; por defecto apagado, y sin él no se registra ningún bean
 * de esta clase). Registra el almacén indicado en {@code auditoria.outbox.almacen} ({@code jdbc} o {@code mongo}, cada
 * uno solo si el servicio trae su librería), un {@link AuditoriaPort} que escribe en él, el relay, la limpieza, su
 * planificador y, si el servicio tiene Micrometer, las gauges del outbox.
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
     * @param publicador     publicador directo, para el modo "solo drenar"
     * @return el puerto
     */
    @Bean
    @ConditionalOnMissingBean(AuditoriaPort.class)
    public OutboxAuditoriaPort outboxAuditoriaPort(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox, @Value(TOPIC) final String topic,
            @Value(SERVICIO) final String servicioOrigen, final ObjectProvider<AuditoriaPublisher> publicador) {
        propiedades.validar();
        AuditoriaPublisher directo = null;
        if (propiedades.isSoloDrenar()) {
            directo = publicador.getObject();
        }
        return new OutboxAuditoriaPort(outbox.getObject(), topic, servicioOrigen, reloj, directo);
    }

    /**
     * Relay del outbox.
     *
     * @param propiedades {@code auditoria.outbox.*}
     * @param outbox      almacén del outbox
     * @param publicador  publicador de auditoría ({@code auditoria.enabled} tiene que seguir activo)
     * @param generico    publicador de los eventos genéricos, si el servicio tiene Kafka
     * @return el relay
     */
    @Bean
    public OutboxAuditoriaRelay outboxAuditoriaRelay(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox, final ObjectProvider<AuditoriaPublisher> publicador,
            final ObjectProvider<PublicadorOutbox> generico) {
        propiedades.validar();
        return new OutboxAuditoriaRelay(outbox.getObject(), publicador.getObject(), generico.getIfAvailable(),
                propiedades, reloj, OutboxAuditoriaRelay.Pausa.DORMIR);
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
     * Estadísticas del outbox para las métricas (solo consultan el almacén si alguien las publica).
     *
     * @param propiedades {@code auditoria.outbox.*}
     * @param outbox      almacén del outbox
     * @return las estadísticas
     */
    @Bean
    public AuditoriaOutboxEstadisticas auditoriaOutboxEstadisticas(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox) {
        propiedades.validar();
        return new AuditoriaOutboxEstadisticas(outbox.getObject(), reloj);
    }

    /**
     * Planificador del relay, de la limpieza y del refresco de las métricas.
     *
     * @param relay        relay del outbox
     * @param limpieza     limpieza del outbox
     * @param propiedades  {@code auditoria.outbox.*}
     * @param estadisticas estadísticas para las métricas
     * @return el planificador
     */
    @Bean
    public AuditoriaOutboxPlanificador auditoriaOutboxPlanificador(final OutboxAuditoriaRelay relay,
            final AuditoriaOutboxLimpieza limpieza, final AuditoriaOutboxProperties propiedades,
            final AuditoriaOutboxEstadisticas estadisticas) {
        return new AuditoriaOutboxPlanificador(relay, limpieza, propiedades, estadisticas);
    }

    /**
     * Puerto del outbox genérico, si el servicio declara sus {@link TiposDeEvento}: los valida al arrancar (topics
     * resueltos, almacén con soporte de eventos genéricos) y rechaza en ejecución los tipos no declarados.
     *
     * @param propiedades    {@code auditoria.outbox.*}
     * @param outbox         almacén del outbox
     * @param tipos          tipos declarados por el servicio
     * @param entorno        entorno, para resolver los topics
     * @param servicioOrigen {@code spring.application.name}
     * @return el puerto
     */
    @Bean
    @ConditionalOnBean(TiposDeEvento.class)
    @ConditionalOnMissingBean(OutboxPort.class)
    public OutboxRegistro outboxPort(final AuditoriaOutboxProperties propiedades,
            final ObjectProvider<AuditoriaOutboxPort> outbox, final TiposDeEvento tipos, final Environment entorno,
            @Value(SERVICIO) final String servicioOrigen) {
        propiedades.validar();
        final AuditoriaOutboxPort almacen = outbox.getObject();
        almacen.verificarEventosGenericos();
        return new OutboxRegistro(almacen, tipos.resolver(entorno::resolveRequiredPlaceholders), servicioOrigen, reloj,
                propiedades.isSoloDrenar());
    }

    /**
     * Publicador de los eventos genéricos: un productor propio con las propiedades {@code spring.kafka.*} del servicio,
     * serializers String, {@code acks=all} e idempotencia (como el de auditoría; no se expone el template). Los tipos
     * declarados como compatibles con {@code JsonSerializer} salen con ese formato.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KafkaTemplate.class)
    @ConditionalOnBean(KafkaProperties.class)
    static class ConKafka {

        @Bean
        @ConditionalOnMissingBean(PublicadorOutbox.class)
        KafkaPublicadorOutbox publicadorOutbox(final KafkaProperties kafkaProperties,
                final ObjectProvider<SslBundles> sslBundles, @Value(SERVICIO) final String servicioOrigen,
                final ObjectProvider<TiposDeEvento> tipos) {
            final Map<String, Object> propiedades = kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable());
            propiedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            propiedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            propiedades.put(ProducerConfig.ACKS_CONFIG, "all");
            propiedades.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            propiedades.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, servicioOrigen + "-outbox");
            return new KafkaPublicadorOutbox(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(propiedades)),
                    tipos.getIfAvailable());
        }
    }

    /** Almacén JDBC: tabla {@code auditoria.outbox.jdbc.tabla} ({@code auditoria_outbox} por defecto). */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(JdbcTemplate.class)
    @ConditionalOnProperty(prefix = AuditoriaOutboxProperties.PREFIJO, name = "almacen", havingValue = "jdbc")
    static class Jdbc {

        @Bean
        @ConditionalOnMissingBean(AuditoriaOutboxPort.class)
        JdbcAuditoriaOutboxAdapter jdbcAuditoriaOutboxAdapter(final DataSource dataSource,
                final AuditoriaOutboxProperties propiedades) {
            return new JdbcAuditoriaOutboxAdapter(dataSource, propiedades.getJdbc().getTabla());
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
            return new MongoAuditoriaOutboxAdapter(mongo, coleccion(propiedades, servicioOrigen),
                    propiedades.getRetencion());
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

    /**
     * Gauges de Micrometer, si el servicio lo tiene. Es un {@code MeterBinder}: los registra el {@code MeterRegistry}
     * de Spring Boot Actuator al arrancar.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterBinder.class)
    static class ConMicrometer {

        @Bean
        AuditoriaOutboxMetricas auditoriaOutboxMetricas(final AuditoriaOutboxEstadisticas estadisticas,
                @Value(SERVICIO) final String servicioOrigen, final ObjectProvider<TiposDeEvento> tipos,
                final ObjectProvider<OutboxPort> puerto) {
            Set<String> declarados = Set.of();
            final TiposDeEvento delServicio = tipos.getIfAvailable();
            if (delServicio != null) {
                declarados = delServicio.nombres();
            }
            ToLongFunction<String> duplicados = null;
            if (puerto.getIfUnique() instanceof OutboxRegistro registro) {
                duplicados = registro::duplicados;
            }
            return new AuditoriaOutboxMetricas(estadisticas, servicioOrigen, declarados, duplicados);
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
