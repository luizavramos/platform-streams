package br.com.motorfraude.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StreamsPropertiesTest {

    private static StreamsSettings settings() {
        return new StreamsSettings("customer-service", "localhost:9092", Path.of("target/state"), 1, 0, "customer-service-0", 8083);
    }

    @Test
    @DisplayName("exactly_once_v2, id da aplicação, servidores e diretório de estado")
    void essencial() {
        var props = StreamsAppBootstrap.properties(settings());

        assertThat(props.get(StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("customer-service");
        assertThat(props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        assertThat(props.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).isEqualTo(StreamsConfig.EXACTLY_ONCE_V2);
        assertThat(props.get(StreamsConfig.STATE_DIR_CONFIG).toString()).endsWith("state");
        assertThat(props.get(StreamsConfig.REPLICATION_FACTOR_CONFIG)).isEqualTo(1);
        assertThat(props.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG)).isEqualTo(0);
    }

    @Test
    @DisplayName("static membership: o group.instance.id do consumidor vem do identificador da instância")
    void staticMembership() {
        var props = StreamsAppBootstrap.properties(settings());

        assertThat(props.get(StreamsConfig.consumerPrefix("group.instance.id"))).isEqualTo("customer-service-0");
    }

    @Test
    @DisplayName("as propriedades são aceitas pelo StreamsConfig do Kafka")
    void configValida() {
        var config = new StreamsConfig(StreamsAppBootstrap.properties(settings()));

        assertThat(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).isEqualTo("exactly_once_v2");
    }

    @Test
    @DisplayName("réplicas de espera e fator de replicação vêm da configuração")
    void producaoComReplicas() {
        var production = new StreamsSettings("detection-engine", "broker:9092", Path.of("/data"), 3, 1, "detection-engine-2", 8084);

        var props = StreamsAppBootstrap.properties(production);

        assertThat(props.get(StreamsConfig.REPLICATION_FACTOR_CONFIG)).isEqualTo(3);
        assertThat(props.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG)).isEqualTo(1);
    }

    @Test
    @DisplayName("configuração inválida é recusada com a mensagem do campo")
    void validacao() {
        assertThatThrownBy(() -> new StreamsSettings(" ", "localhost:9092", Path.of("x"), 1, 0, "i", 8080)).hasMessageContaining("applicationId");
        assertThatThrownBy(() -> new StreamsSettings("a", "", Path.of("x"), 1, 0, "i", 8080)).hasMessageContaining("bootstrapServers");
        assertThatThrownBy(() -> new StreamsSettings("a", "b", Path.of("x"), 0, 0, "i", 8080)).hasMessageContaining("replicationFactor");
        assertThatThrownBy(() -> new StreamsSettings("a", "b", Path.of("x"), 1, 0, "", 8080)).hasMessageContaining("instanceId");
    }

    @Test
    @DisplayName("a partir das variáveis de ambiente: HOSTNAME vira o identificador da instância")
    void deAmbiente() {
        var env = java.util.Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092", "HOSTNAME", "pod-7", "HTTP_PORT", "8083", "STATE_DIR", "/data");

        var s = StreamsSettings.fromEnvironment("customer-service", env);

        assertThat(s.bootstrapServers()).isEqualTo("kafka:29092");
        assertThat(s.instanceId()).isEqualTo("pod-7");
        assertThat(s.httpPort()).isEqualTo(8083);
        assertThat(s.stateDir()).isEqualTo(Path.of("/data"));
        assertThat(s.replicationFactor()).isEqualTo(1);
    }
}
