package br.com.motorfraude.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class StreamsAppBootstrapIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    private static String awaitValue(String topic, Duration timeout) {
        var props = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            var deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(300))) {
                    return record.value();
                }
            }
        }
        return null;
    }

    private static String http(int port, String path) throws Exception {
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return response.statusCode() + " " + response.body();
    }

    @Test
    @DisplayName("sobe a topologia, fica pronto, processa com exactly-once, expõe saúde e métricas e fecha limpo")
    void ciclodeVida(@TempDir Path tmp) throws Exception {
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("in", 1, (short) 1), new NewTopic("out", 1, (short) 1))).all().get();
        }
        var builder = new StreamsBuilder();
        builder.stream("in", Consumed.with(Serdes.String(), Serdes.String())).mapValues(v -> v.toString().toUpperCase()).to("out", Produced.with(Serdes.String(), Serdes.String()));
        var settings = new StreamsSettings("bootstrap-it", KAFKA.getBootstrapServers(), tmp, 1, 0, "bootstrap-it-0", 0);

        try (var app = StreamsAppBootstrap.create(settings, builder.build())) {
            assertThat(app.ready()).isFalse();
            app.start();
            for (int i = 0; i < 100 && !app.ready(); i++) {
                Thread.sleep(200);
            }

            assertThat(app.ready()).isTrue();
            assertThat(app.live()).isTrue();
            assertThat(app.state()).isEqualTo(KafkaStreams.State.RUNNING);
            assertThat(http(app.httpPort(), "/actuator/health/readiness")).startsWith("200 ").contains("UP");

            try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
                producer.send(new ProducerRecord<>("in", "k", "abc")).get();
            }
            assertThat(awaitValue("out", Duration.ofSeconds(30))).isEqualTo("ABC");

            var metrics = http(app.httpPort(), "/actuator/prometheus");
            assertThat(metrics).startsWith("200 ").contains("kafka_stream_");

            try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                var group = admin.describeConsumerGroups(List.of("bootstrap-it")).all().get().get("bootstrap-it");
                assertThat(group.members()).isNotEmpty();
                assertThat(group.members()).allSatisfy(m -> assertThat(m.groupInstanceId()).hasValueSatisfying(id -> assertThat(id).startsWith("bootstrap-it-0")));
            }

            app.close();
            assertThat(app.state()).isEqualTo(KafkaStreams.State.NOT_RUNNING);
            assertThat(app.live()).isFalse();
        }
    }
}
