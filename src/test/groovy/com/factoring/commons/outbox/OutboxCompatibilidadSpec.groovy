package com.factoring.commons.outbox

import com.factoring.commons.auditoria.AuditoriaAutoConfiguration
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxAutoConfiguration
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxEvento
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxMetricas
import com.factoring.commons.auditoria.outbox.AuditoriaOutboxPort
import com.factoring.commons.auditoria.outbox.RelojDePrueba
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import spock.lang.Specification

/** 0.6.1 sin almacén ni broker reales: declaración de los tipos compatibles, puerto por defecto y autoconfiguración. */
class OutboxCompatibilidadSpec extends Specification {

    static TipoDeEvento.Builder<Map> base(String nombre = "documento.creado") {
        TipoDeEvento.de(nombre, Map).topic("documentos").clave({ Map m -> m.id as String })
    }

    def "por defecto un tipo sale como sobre; compatibleConJsonSerializer() usa el nombre de la clase y (alias) el alias"() {
        expect:
        !base().build().compatibleConJsonSerializer()
        base().build().idTipoCompatible() == null
        base().compatibleConJsonSerializer().build().idTipoCompatible() == Map.name
        base().compatibleConJsonSerializer("documento").build().idTipoCompatible() == "documento"
        base().compatibleConJsonSerializer("documento").build().compatibleConJsonSerializer()
    }

    def "el constructor de 5 argumentos de 0.6.0 sigue existiendo y deja el tipo como sobre"() {
        when:
        def tipo = new TipoDeEvento<Map>("documento.creado", 1, Map, "documentos", { Map m -> "k" })

        then:
        !tipo.compatibleConJsonSerializer()
    }

    def "un __TypeId__ vacío o nulo falla al declarar los tipos"() {
        when:
        TiposDeEvento.de(base().compatibleConJsonSerializer(id).build())

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("__TypeId__")

        where:
        id << [null, "", "  "]
    }

    def "resolver los topics conserva la opción de compatibilidad"() {
        when:
        def resueltos = TiposDeEvento.de(TipoDeEvento.de("documento.creado", Map).topic('${t}')
                .clave({ Map m -> "k" }).compatibleConJsonSerializer("documento").build()).resolver({ "documentos.v1" })

        then:
        resueltos.requerir("documento.creado").topic() == "documentos.v1"
        resueltos.requerir("documento.creado").idTipoCompatible() == "documento"
    }

    def "un OutboxPort propio que no implementa la sobrecarga lanza UnsupportedOperationException"() {
        given:
        OutboxPort propio = { String tipo, Object payload -> null } as OutboxPort

        when:
        propio.registrar("documento.creado", "ID-1", [id: "1"])

        then:
        thrown(UnsupportedOperationException)
    }

    def "un almacén propio sin guardarSiNoExiste: el registro con eventId propio usa su guardar"() {
        given:
        AuditoriaOutboxPort almacen = Mock()
        def registro = new OutboxRegistro(almacen, TiposDeEvento.de(base().build()), "ms-prueba", new RelojDePrueba(),
                false)

        when:
        def evento = registro.registrar("documento.creado", "ID-1", [id: "1"])

        then:
        1 * almacen.guardarSiNoExiste(_) >> { callRealMethod() }
        evento.get().eventId() == "ID-1"
        1 * almacen.guardar({ AuditoriaOutboxEvento e -> e.eventId() == "ID-1" && e.tipo() == "documento.creado" })
    }

    def "la sobrecarga respeta las mismas reglas: tipo no declarado, payload de otra clase y solo drenar"() {
        given:
        AuditoriaOutboxPort almacen = Mock()
        def tipos = TiposDeEvento.de(base().build())

        when:
        new OutboxRegistro(almacen, tipos, "ms-prueba", new RelojDePrueba(), false).registrar("otro", "ID", [id: "1"])

        then:
        thrown(TipoDeEventoNoDeclaradoException)

        when:
        new OutboxRegistro(almacen, tipos, "ms-prueba", new RelojDePrueba(), false)
                .registrar("documento.creado", "ID", "texto")

        then:
        thrown(IllegalArgumentException)

        when:
        new OutboxRegistro(almacen, tipos, "ms-prueba", new RelojDePrueba(), true)
                .registrar("documento.creado", "ID", [id: "1"])

        then:
        thrown(OutboxSoloDrenarException)
        0 * almacen._
    }

    def "autoconfiguración: el publicador recibe los tipos declarados y se publica el contador outbox.duplicados"() {
        given:
        AuditoriaOutboxPort almacen = Stub() {
            guardarSiNoExiste(_) >> false
        }
        def runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration,
                        AuditoriaOutboxAutoConfiguration, MetricsAutoConfiguration,
                        SimpleMetricsExportAutoConfiguration, CompositeMeterRegistryAutoConfiguration))
                .withPropertyValues(OutboxGenericoAutoConfigurationSpec.BASE)
                .withBean("outbox", AuditoriaOutboxPort, { almacen })
                .withBean(TiposDeEvento, { TiposDeEvento.de(base().compatibleConJsonSerializer("documento").build()) })

        expect:
        runner.run { ctx ->
            assert ctx.startupFailure == null
            def publicador = ctx.getBean(PublicadorOutbox) as KafkaPublicadorOutbox
            assert publicador.@compatibles.keySet() == ["documento.creado"] as Set
            assert ctx.getBean(OutboxPort).registrar("documento.creado", "ID-1", [id: "1"]).isEmpty()
            assert ctx.getBean(MeterRegistry).get(AuditoriaOutboxMetricas.OUTBOX_DUPLICADOS)
                    .tag("tipo", "documento.creado").tag("servicio", "ms-prueba").functionCounter().count() == 1d
        }
    }
}
