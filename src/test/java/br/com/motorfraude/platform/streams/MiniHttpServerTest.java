package br.com.motorfraude.platform.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MiniHttpServerTest {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final AtomicBoolean live = new AtomicBoolean(true);
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final MiniHttpServer server = new MiniHttpServer(0, registry, live::get, ready::get).start();
    private final HttpClient client = HttpClient.newHttpClient();

    @AfterEach
    void stop() {
        server.close();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @DisplayName("liveness: 200 UP enquanto o app vive, 503 DOWN quando não")
    void liveness() throws Exception {
        var up = get("/actuator/health/liveness");
        assertThat(up.statusCode()).isEqualTo(200);
        assertThat(up.body()).contains("\"status\":\"UP\"");
        assertThat(up.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).startsWith("application/json"));

        live.set(false);

        var down = get("/actuator/health/liveness");
        assertThat(down.statusCode()).isEqualTo(503);
        assertThat(down.body()).contains("\"status\":\"DOWN\"");
    }

    @Test
    @DisplayName("readiness: 503 OUT_OF_SERVICE até ficar pronto, depois 200 UP")
    void readiness() throws Exception {
        var notReady = get("/actuator/health/readiness");
        assertThat(notReady.statusCode()).isEqualTo(503);
        assertThat(notReady.body()).contains("\"status\":\"OUT_OF_SERVICE\"");

        ready.set(true);

        var ok = get("/actuator/health/readiness");
        assertThat(ok.statusCode()).isEqualTo(200);
        assertThat(ok.body()).contains("\"status\":\"UP\"");
    }

    @Test
    @DisplayName("saúde agregada: DOWN se não vive, OUT_OF_SERVICE se não está pronto, UP se está tudo bem")
    void saudeAgregada() throws Exception {
        assertThat(get("/actuator/health").body()).contains("OUT_OF_SERVICE");
        ready.set(true);
        assertThat(get("/actuator/health").statusCode()).isEqualTo(200);
        assertThat(get("/actuator/health").body()).contains("\"status\":\"UP\"");
        live.set(false);
        assertThat(get("/actuator/health").statusCode()).isEqualTo(503);
        assertThat(get("/actuator/health").body()).contains("\"status\":\"DOWN\"");
    }

    @Test
    @DisplayName("/actuator/prometheus expõe o registro no formato de texto do Prometheus")
    void metricas() throws Exception {
        Counter.builder("customer.profiles.published").register(registry).increment(3);

        var response = get("/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).startsWith("text/plain"));
        assertThat(response.body()).contains("customer_profiles_published_total 3.0");
    }

    @Test
    @DisplayName("rota desconhecida é 404 e método diferente de GET é 405")
    void rotasInvalidas() throws Exception {
        assertThat(get("/metrics").statusCode()).isEqualTo(404);
        assertThat(get("/nada").statusCode()).isEqualTo(404);
        var post = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/actuator/health"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(post.statusCode()).isEqualTo(405);
    }

    @Test
    @DisplayName("porta 0 escolhe uma porta livre; depois de fechar, a porta deixa de responder")
    void portaELimpeza() throws Exception {
        assertThat(server.port()).isPositive();
        int port = server.port();

        server.close();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString())).isInstanceOf(java.io.IOException.class);
    }
}
