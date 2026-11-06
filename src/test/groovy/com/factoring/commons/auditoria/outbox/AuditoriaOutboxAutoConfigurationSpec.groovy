package com.factoring.commons.auditoria.outbox

import com.factoring.commons.auditoria.AuditoriaAutoConfiguration
import com.factoring.commons.auditoria.AuditoriaPort
import com.factoring.commons.auditoria.outbox.jdbc.JdbcAuditoriaOutboxAdapter
import com.factoring.commons.auditoria.outbox.mongo.MongoAuditoriaOutboxAdapter
import net.javacrumbs.shedlock.core.LockProvider
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.index.IndexOperations
import org.springframework.jdbc.core.JdbcTemplate
import spock.lang.Specification

import javax.sql.DataSource

class AuditoriaOutboxAutoConfigurationSpec extends Specification {

    static final String[] BASE = ["spring.kafka.bootstrap-servers=localhost:1", "spring.application.name=ms-prueba",
                                  // el relay no llega a correr durante el test
                                  "auditoria.outbox.intervalo=1h"]

    ApplicationContextRunner sinOutbox = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration))
            .withPropertyValues(BASE)

    ApplicationContextRunner conOutbox = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration, AuditoriaAutoConfiguration,
                    AuditoriaOutboxAutoConfiguration))
            .withPropertyValues(BASE)

    private MongoOperations mongoDePrueba() {
        Stub(MongoOperations) {
            collectionExists(_) >> false
            indexOps(_ as String) >> Mock(IndexOperations)
        }
    }

    def "con enabled=false (o sin configurar) no se registra ningún bean nuevo respecto de 0.4.x"() {
        given:
        DataSource dataSource = Mock()
        Set<String> antes = null
        Set<String> despues = null

        when:
        sinOutbox.withBean("dataSource", DataSource, { dataSource }).run { ctx -> antes = ctx.beanDefinitionNames as Set }
        conOutbox.withBean("dataSource", DataSource, { dataSource }).withPropertyValues(propiedades)
                .run { ctx -> despues = ctx.beanDefinitionNames as Set }

        then:
        despues == antes
        antes.every { !it.toLowerCase().contains("outbox") }

        where:
        propiedades << [[] as String[], ["auditoria.outbox.enabled=false", "auditoria.outbox.almacen=jdbc"] as String[]]
    }

    def "jdbc: registra el almacén JDBC, el puerto, el relay, la limpieza sin bloqueo y el planificador"() {
        expect:
        conOutbox.withBean("dataSource", DataSource, { Mock(DataSource) })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=jdbc")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    assert ctx.getBean(AuditoriaOutboxPort) instanceof JdbcAuditoriaOutboxAdapter
                    assert ctx.getBean(AuditoriaPort) instanceof OutboxAuditoriaPort
                    assert ctx.getBeansOfType(OutboxAuditoriaRelay).size() == 1
                    assert ctx.getBeansOfType(AuditoriaOutboxLimpieza).size() == 1
                    assert ctx.getBean(AuditoriaOutboxPlanificador).running
                    assert ctx.getBeansOfType(BloqueoLimpieza).isEmpty()
                }
    }

    def "mongo: colección por defecto con el nombre del servicio, índices creados al arrancar"() {
        given:
        IndexOperations indices = Mock()
        MongoOperations mongo = Stub() {
            collectionExists(_) >> false
            indexOps("auditoria_outbox_ms_prueba") >> indices
        }

        expect:
        conOutbox.withBean("mongoTemplate", MongoOperations, { mongo })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=mongo")
                .run { ctx ->
                    assert ctx.startupFailure == null
                    def adaptador = ctx.getBean(AuditoriaOutboxPort)
                    assert adaptador instanceof MongoAuditoriaOutboxAdapter
                    assert adaptador.coleccion == "auditoria_outbox_ms_prueba"
                }
    }

    def "mongo: la colección se puede configurar"() {
        expect:
        conOutbox.withBean("mongoTemplate", MongoOperations, { mongoDePrueba() })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=mongo",
                        "auditoria.outbox.mongo.coleccion=outbox_propio")
                .run { ctx -> assert ctx.getBean(MongoAuditoriaOutboxAdapter).coleccion == "outbox_propio" }
    }

    def "enabled=true sin almacen falla al arrancar con un mensaje claro"() {
        expect:
        conOutbox.withPropertyValues("auditoria.outbox.enabled=true").run { ctx ->
            assert ctx.startupFailure != null
            assert causas(ctx.startupFailure).any { it.message?.contains("auditoria.outbox.almacen es obligatorio") }
        }
    }

    def "almacen=jdbc sin spring-jdbc en el classpath no registra el almacén y el arranque falla"() {
        expect:
        conOutbox.withClassLoader(new FilteredClassLoader(JdbcTemplate))
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=jdbc")
                .run { ctx -> assert ctx.startupFailure != null }
    }

    def "un servicio con solo Mongo no necesita JDBC, y uno con solo JDBC no necesita Mongo"() {
        expect:
        conOutbox.withClassLoader(new FilteredClassLoader(JdbcTemplate))
                .withBean("mongoTemplate", MongoOperations, { mongoDePrueba() })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=mongo")
                .run { ctx -> assert ctx.getBean(AuditoriaOutboxPort) instanceof MongoAuditoriaOutboxAdapter }
        conOutbox.withClassLoader(new FilteredClassLoader(MongoOperations))
                .withBean("dataSource", DataSource, { Mock(DataSource) })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=jdbc")
                .run { ctx -> assert ctx.getBean(AuditoriaOutboxPort) instanceof JdbcAuditoriaOutboxAdapter }
    }

    def "con un LockProvider de ShedLock en el servicio, la limpieza lo usa"() {
        expect:
        conOutbox.withBean("dataSource", DataSource, { Mock(DataSource) })
                .withBean("lockProvider", LockProvider, { Mock(LockProvider) })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=jdbc")
                .run { ctx -> assert ctx.getBean(BloqueoLimpieza) instanceof ShedLockBloqueoLimpieza }
    }

    def "un AuditoriaPort propio del servicio no se reemplaza"() {
        given:
        AuditoriaPort propio = Mock()

        expect:
        conOutbox.withBean("dataSource", DataSource, { Mock(DataSource) })
                .withBean("propio", AuditoriaPort, { propio })
                .withPropertyValues("auditoria.outbox.enabled=true", "auditoria.outbox.almacen=jdbc")
                .run { ctx -> assert ctx.getBeansOfType(AuditoriaPort).values() as List == [propio] }
    }

    private static List<Throwable> causas(Throwable error) {
        def lista = []
        for (Throwable t = error; t != null; t = t.cause) {
            lista << t
        }
        lista
    }
}
