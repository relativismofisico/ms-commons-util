package com.factoring.commons.auditoria;

import java.util.Map;

import com.factoring.commons.auditoria.outbox.AuditoriaOutboxProperties;
import com.factoring.commons.outbox.ProductorOutbox;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Registra {@link AuditoriaPublisher} en todo servicio con Kafka ({@code auditoria.enabled=false} lo apaga).
 *
 * <p>El productor sale de las mismas propiedades {@code spring.kafka.*} del servicio (bootstrap, seguridad), pero con
 * serializers String propios, {@code acks=all} e idempotencia: el {@code ProducerFactory} y el {@code KafkaTemplate}
 * NO se exponen como beans, para no competir por tipo con los que cada servicio ya define.</p>
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBean(KafkaProperties.class)
@ConditionalOnProperty(prefix = "auditoria", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AuditoriaAutoConfiguration {

    /**
     * Publicador de auditoría.
     *
     * @param kafkaProperties propiedades {@code spring.kafka.*} del servicio
     * @param sslBundles      bundles SSL, si el servicio los define
     * @param topic           {@code auditoria.topic}
     * @param servicioOrigen  {@code spring.application.name}
     * @param outbox         {@code auditoria.outbox.*}, solo si el outbox está activo: entonces el relay espera cada
     *                        envío con {@code timeout-ack} y el productor se acota a ese plazo (desde 0.6.2)
     * @return el publicador
     */
    @Bean
    @ConditionalOnMissingBean(AuditoriaPublisher.class)
    public KafkaAuditoriaPublisher auditoriaPublisher(final KafkaProperties kafkaProperties,
            final ObjectProvider<SslBundles> sslBundles,
            @Value("${auditoria.topic:" + AuditoriaConstantes.TOPIC_POR_DEFECTO + "}") final String topic,
            @Value("${spring.application.name:desconocido}") final String servicioOrigen,
            final ObjectProvider<AuditoriaOutboxProperties> outbox) {
        final Map<String, Object> propiedades = kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable());
        propiedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propiedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propiedades.put(ProducerConfig.ACKS_CONFIG, "all");
        propiedades.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        propiedades.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, servicioOrigen + "-auditoria");
        final AuditoriaOutboxProperties conOutbox = outbox.getIfAvailable();
        if (conOutbox != null) {
            ProductorOutbox.acotarTimeouts(propiedades, conOutbox.getTimeoutAck());
        }
        final KafkaTemplate<String, String> template =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(propiedades));
        return new KafkaAuditoriaPublisher(template, topic, servicioOrigen);
    }
}
