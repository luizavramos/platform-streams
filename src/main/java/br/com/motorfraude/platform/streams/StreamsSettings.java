package br.com.motorfraude.platform.streams;

import java.nio.file.Path;
import java.util.Map;

public record StreamsSettings(String applicationId, String bootstrapServers, Path stateDir, int replicationFactor,
        int standbyReplicas, String instanceId, int httpPort) {

    public StreamsSettings {
        if (applicationId == null || applicationId.isBlank()) {
            throw new IllegalArgumentException("applicationId é obrigatório");
        }
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers é obrigatório");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor deve ser 1 ou mais");
        }
        if (standbyReplicas < 0) {
            throw new IllegalArgumentException("standbyReplicas não pode ser negativo");
        }
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId é obrigatório");
        }
    }

    public static StreamsSettings fromEnvironment(String applicationId, Map<String, String> env) {
        return new StreamsSettings(
                applicationId,
                env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                Path.of(env.getOrDefault("STATE_DIR", "/data")),
                Integer.parseInt(env.getOrDefault("REPLICATION_FACTOR", "1")),
                Integer.parseInt(env.getOrDefault("STANDBY_REPLICAS", "0")),
                env.getOrDefault("HOSTNAME", applicationId + "-0"),
                Integer.parseInt(env.getOrDefault("HTTP_PORT", "8080")));
    }
}
