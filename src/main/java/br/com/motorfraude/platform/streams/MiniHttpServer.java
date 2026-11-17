package br.com.motorfraude.platform.streams;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

/**
 * Servidor HTTP mínimo (o do JDK, sem Spring) com os mesmos caminhos dos serviços Spring (constituição VII):
 * {@code /actuator/health}, {@code /actuator/health/liveness}, {@code /actuator/health/readiness} e
 * {@code /actuator/prometheus}.
 */
public final class MiniHttpServer implements AutoCloseable {

    private static final String PROMETHEUS_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private static final String SWAGGER_PAGE = """
            <!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>API</title>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.17.14/swagger-ui.min.css"></head>
            <body><div id="ui"></div>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.17.14/swagger-ui-bundle.min.js"></script>
            <script>window.onload=()=>SwaggerUIBundle({url:'/v3/api-docs',dom_id:'#ui'});</script></body></html>
            """;

    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final PrometheusMeterRegistry registry;
    private final BooleanSupplier live;
    private final BooleanSupplier ready;

    public MiniHttpServer(int port, PrometheusMeterRegistry registry, BooleanSupplier live, BooleanSupplier ready) {
        this.registry = registry;
        this.live = live;
        this.ready = ready;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new IllegalStateException("não foi possível abrir a porta " + port, e);
        }
        server.setExecutor(executor);
        server.createContext("/", this::handle);
    }

    public MiniHttpServer start() {
        server.start();
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                reply(exchange, 405, "text/plain; charset=utf-8", "método não permitido");
                return;
            }
            switch (exchange.getRequestURI().getPath()) {
                case "/actuator/health/liveness" -> reply(exchange, live.getAsBoolean() ? 200 : 503, json(live.getAsBoolean() ? "UP" : "DOWN"));
                case "/actuator/health/readiness" -> reply(exchange, ready.getAsBoolean() ? 200 : 503, json(ready.getAsBoolean() ? "UP" : "OUT_OF_SERVICE"));
                case "/actuator/health" -> {
                    if (!live.getAsBoolean()) {
                        reply(exchange, 503, json("DOWN"));
                    } else if (!ready.getAsBoolean()) {
                        reply(exchange, 503, json("OUT_OF_SERVICE"));
                    } else {
                        reply(exchange, 200, json("UP"));
                    }
                }
                case "/actuator/prometheus" -> reply(exchange, 200, PROMETHEUS_TYPE, registry.scrape());
                case "/v3/api-docs" -> {
                    var doc = openApi();
                    if (doc == null) {
                        reply(exchange, 404, "text/plain; charset=utf-8", "não encontrado");
                    } else {
                        reply(exchange, 200, "application/json", doc);
                    }
                }
                case "/swagger-ui.html" -> reply(exchange, 200, "text/html; charset=utf-8", SWAGGER_PAGE);
                default -> reply(exchange, 404, "text/plain; charset=utf-8", "não encontrado");
            }
        }
    }

    /** Documento OpenAPI do serviço: o recurso {@code openapi.json} do classpath da aplicação (null se não houver). */
    private static String openApi() throws IOException {
        var loader = Thread.currentThread().getContextClassLoader();
        try (var in = (loader != null ? loader : MiniHttpServer.class.getClassLoader()).getResourceAsStream("openapi.json")) {
            return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String json(String status) {
        return "{\"status\":\"" + status + "\"}";
    }

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        reply(exchange, status, "application/json", body);
    }

    private static void reply(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        var bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
