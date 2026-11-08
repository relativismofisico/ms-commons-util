package com.factoring.commons.outbox.idempotencia;

import java.time.Clock;
import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Consumo idempotente ({@code consumo-idempotente.enabled=true}; por defecto apagado). Registra el
 * {@link ConsumoIdempotente} del almacén indicado en {@code consumo-idempotente.almacen}: JDBC (con su limpieza
 * periódica) o Mongo (con índice TTL). Usa el DataSource, el gestor de transacciones y el {@code MongoTemplate} del
 * servicio; no expone gestores propios.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
        "org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration"})
@ConditionalOnProperty(prefix = ConsumoIdempotenteProperties.PREFIJO, name = "enabled", havingValue = "true")
@EnableConfigurationProperties(ConsumoIdempotenteProperties.class)
public class ConsumoIdempotenteAutoConfiguration {

    private static final Clock RELOJ = Clock.systemUTC();

    /** Almacén JDBC: tabla en el DataSource del servicio, con su gestor de transacciones. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(JdbcTemplate.class)
    @ConditionalOnProperty(prefix = ConsumoIdempotenteProperties.PREFIJO, name = "almacen", havingValue = "jdbc")
    static class Jdbc {

        @Bean
        @ConditionalOnMissingBean(ConsumoIdempotente.class)
        JdbcConsumoIdempotente consumoIdempotente(final ConsumoIdempotenteProperties propiedades,
                final DataSource dataSource, final PlatformTransactionManager transacciones) {
            propiedades.validar();
            return new JdbcConsumoIdempotente(dataSource, transacciones, propiedades.getTabla(), RELOJ);
        }

        @Bean
        LimpiezaConsumoIdempotente limpiezaConsumoIdempotente(final ConsumoIdempotenteProperties propiedades,
                final ConsumoIdempotente consumo) {
            return new LimpiezaConsumoIdempotente(consumo, propiedades.getRetencion(),
                    propiedades.getIntervaloLimpieza(), RELOJ);
        }
    }

    /**
     * Almacén Mongo: colección del servicio. Usa el {@code MongoTransactionManager} del servicio si lo expone; si no
     * (p. ej. un servicio con JPA como gestor principal), crea uno propio, no expuesto, sobre la misma base.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MongoOperations.class)
    @ConditionalOnProperty(prefix = ConsumoIdempotenteProperties.PREFIJO, name = "almacen", havingValue = "mongo")
    static class Mongo {

        @Bean(initMethod = "crearIndices")
        @ConditionalOnMissingBean(ConsumoIdempotente.class)
        MongoConsumoIdempotente consumoIdempotente(final ConsumoIdempotenteProperties propiedades,
                final MongoOperations mongo, final ObjectProvider<MongoTransactionManager> transacciones,
                final MongoDatabaseFactory baseDeDatos) {
            propiedades.validar();
            final MongoTransactionManager gestor = transacciones.getIfAvailable(
                    () -> new MongoTransactionManager(baseDeDatos));
            return new MongoConsumoIdempotente(mongo, gestor, propiedades.getColeccion(), propiedades.getRetencion(),
                    RELOJ);
        }
    }
}
