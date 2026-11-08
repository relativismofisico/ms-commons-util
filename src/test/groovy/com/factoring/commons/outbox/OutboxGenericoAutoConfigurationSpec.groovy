package com.factoring.commons.outbox

import com.factoring.commons.auditoria.AuditoriaAutoConfiguration
import com.factoring.commons.auditoria.AuditoriaPort
import com.factoring.commons.auditoria.AuditoriaPublisher
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxAutoConfiguration
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEstadisticas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxMetricas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.OutboxAuditoriaRelay
import com.factoring.commons.auditoria.outbox.ResumenOutbox
import com.factoring.commons.auditoria.outbox.ResumenTipo
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.factoring.commons.outbox.idempotencia.ConsumoIdempotente
import com.factoring.commons.outbox.idempotencia.ConsumoIdempotenteAutoConfiguration
import com.factoring.commons.outbox.idempotencia.JdbcConsumoIdempotente
import com.factoring.commons.outbox.idempotencia.LimpiezaConsumoIdempotente
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.transaction.PlatformTransactionManager
import spock.lang.Specification

import javax.sql.DataSource

class OutboxGenericoAutoConfigurationSpec extends Specification {

    static final String[] BASE = ["spring.kafka.bootstrap-servers=localhost:1", "spring.application.name=ms-prueba",
                                  "auditoria.outbox.intervalo=1h", "auditoria.outbox.enabled=true",
                                  "auditoria.outbox.almacen=jdbc"]

    ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration,
                    AuditoriaOutboxAutoConfiguration))
            .withPropertyValues(BASE)

    static TiposDeEvento tipos() {
        TiposDeEvento.de(TipoDeEvento.de("documento.creado", Map).topic('${topics.documentos}')
                .clave({ Map m -> m.id as String }).build())
    }

    def "con TiposDeEvento registra el OutboxPort con los topics resueltos y el publicador genérico del relay"() {
        given:
        AuditoriaOutboxPort almacen = Mock()

        when:
        runner.withBean("outbox", AuditoriaOutboxPort, { almacen })
                .withBean(TiposDeEvento, { tipos() })
                .withPropertyValues("topics.documentos=documentos.v1")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    def puerto = ctx.getBean(OutboxPort)
                    puerto.registrar("documento.creado", [id: "1"])
                    assert ctx.getBean(PublicadorOutbox) instanceof KafkaPublicadorOutbox
                    assert ctx.getBeansOfType(OutboxAuditoriaRelay).size() == 1
                }

        then:
        1 * almacen.verificarEventosGenericos()
        1 * almacen.guardar({ it.topic() == "documentos.v1" && it.tipo() == "documento.creado" })
    }

    def "un topic sin su propiedad, o un almacén sin soporte de eventos genéricos, hacen fallar el arranque"() {
        given:
        AuditoriaOutboxPort almacen = Stub() {
            verificarEventosGenericos() >> { if (!soportaTipo) { throw new IllegalStateException("sin columna tipo") } }
        }

        expect:
        runner.withBean("outbox", AuditoriaOutboxPort, { almacen })
                .withBean(TiposDeEvento, { tipos() })
                .withPropertyValues(propiedades as String[])
                .run { ctx -> assert ctx.startupFailure != null }

        where:
        soportaTipo | propiedades
        true        | []
        false       | ["topics.documentos=documentos.v1"]
    }

    def "sin TiposDeEvento no hay OutboxPort (el servicio sigue solo con auditoría)"() {
        expect:
        runner.withBean("outbox", AuditoriaOutboxPort, { Mock(AuditoriaOutboxPort) })
                .run { ctx ->
                    assert ctx.startupFailure == null
                    assert ctx.getBeansOfType(OutboxPort).isEmpty()
                }
    }

    def "jdbc.tabla elige la tabla del almacén JDBC (auditoria_outbox por defecto)"() {
        expect:
        runner.withBean("dataSource", DataSource, { Mock(DataSource) })
                .withPropertyValues(propiedades as String[])
                .run { ctx -> assert (ctx.getBean(AuditoriaOutboxPort) as JdbcAuditoriaOutboxAdapter).tabla == tabla }

        where:
        propiedades                                       || tabla
        []                                                || "auditoria_outbox"
        ["auditoria.outbox.jdbc.tabla=outbox_ms_prueba"]  || "outbox_ms_prueba"
    }

    def "solo-drenar: el puerto de auditoría publica directo y no escribe en el outbox"() {
        given:
        AuditoriaOutboxPort almacen = Mock()
        AuditoriaPublisher publicador = Mock()

        when:
        runner.withBean("outbox", AuditoriaOutboxPort, { almacen })
                .withBean("auditoriaPublisher", AuditoriaPublisher, { publicador })
                .withPropertyValues("auditoria.outbox.solo-drenar=true")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    ctx.getBean(AuditoriaPort).registrar(com.factoring.commons.auditoria.outbox.OutboxIntegracionBase
                            .evento("S-1"))
                }

        then:
        1 * publicador.publicar(_)
        0 * almacen.guardar(_)
    }

    def "gauges outbox.* por tipo (auditoría y los declarados) con tags tipo y servicio, y los alias auditoria.outbox.*"() {
        given:
        AuditoriaOutboxPort almacen = Stub() {
            resumen() >> new ResumenOutbox(5, 1, null)
            resumenPorTipo() >> ["auditoria.accion": new ResumenTipo(2, 0, 1, null),
                                 "documento.creado": new ResumenTipo(3, 1, 0, null)]
        }

        expect:
        runner.withConfiguration(AutoConfigurations.of(MetricsAutoConfiguration, SimpleMetricsExportAutoConfiguration,
                CompositeMeterRegistryAutoConfiguration))
                .withBean("outbox", AuditoriaOutboxPort, { almacen })
                .withBean(TiposDeEvento, { tipos() })
                .withPropertyValues("topics.documentos=documentos.v1")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    def registro = ctx.getBean(MeterRegistry)
                    ctx.getBean(AuditoriaOutboxEstadisticas).actualizarSeguro()
                    def valor = { String nombre, String tipo ->
                        registro.get(nombre).tag("tipo", tipo).tag("servicio", "ms-prueba").gauge().value()
                    }
                    assert valor(AuditoriaOutboxMetricas.OUTBOX_PENDIENTES, "documento.creado") == 3
                    assert valor(AuditoriaOutboxMetricas.OUTBOX_EN_PROCESO, "documento.creado") == 1
                    assert valor(AuditoriaOutboxMetricas.OUTBOX_FALLIDOS, "auditoria.accion") == 1
                    assert registro.get(AuditoriaOutboxMetricas.PENDIENTES).tag("servicio", "ms-prueba").gauge()
                            .value() == 5
                }
    }

    def "consumo-idempotente apagado por defecto; jdbc registra el consumo y su limpieza; sin almacén no arranca"() {
        given:
        def consumo = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConsumoIdempotenteAutoConfiguration))
                .withBean("dataSource", DataSource, { Mock(DataSource) })
                .withBean("transactionManager", PlatformTransactionManager, { Mock(PlatformTransactionManager) })

        expect:
        consumo.run { ctx -> assert ctx.getBeansOfType(ConsumoIdempotente).isEmpty() }
        consumo.withPropertyValues("consumo-idempotente.enabled=true", "consumo-idempotente.almacen=jdbc")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    assert ctx.getBean(ConsumoIdempotente) instanceof JdbcConsumoIdempotente
                    assert ctx.getBean(LimpiezaConsumoIdempotente).running
                }
        consumo.withPropertyValues("consumo-idempotente.enabled=true", "consumo-idempotente.almacen=jdbc",
                "consumo-idempotente.retencion=0s")
                .run { ctx -> assert ctx.startupFailure != null }
    }
}
