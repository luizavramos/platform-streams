package br.com.motorfraude.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StreamsAppBootstrapFactoryTest {

    @Test
    @DisplayName("quem monta a topologia recebe o mesmo registro de métricas que o app expõe em /actuator/prometheus")
    void topologiaRecebeORegistro(@TempDir Path tmp) {
        var received = new AtomicReference<PrometheusMeterRegistry>();
        var settings = new StreamsSettings("factory-test", "localhost:9092", tmp, 1, 0, "factory-test-0", 0);

        try (var app = StreamsAppBootstrap.create(settings, registry -> {
            received.set(registry);
            var builder = new StreamsBuilder();
            builder.stream("in", Consumed.with(Serdes.String(), Serdes.String())).to("out");
            return builder.build();
        })) {
            assertThat(received.get()).isNotNull().isSameAs(app.registry());
        }
    }

    @Test
    @DisplayName("um registro de métricas criado por quem chama é o que o app usa e expõe")
    void registroDeFora(@TempDir Path tmp) {
        var mine = new PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT);
        var received = new AtomicReference<PrometheusMeterRegistry>();
        var settings = new StreamsSettings("factory-test-2", "localhost:9092", tmp, 1, 0, "factory-test-2-0", 0);

        try (var app = StreamsAppBootstrap.create(settings, new java.util.Properties(), mine, registry -> {
            received.set(registry);
            var builder = new StreamsBuilder();
            builder.stream("in", Consumed.with(Serdes.String(), Serdes.String())).to("out");
            return builder.build();
        })) {
            assertThat(app.registry()).isSameAs(mine);
            assertThat(received.get()).isSameAs(mine);
        }
    }
}
