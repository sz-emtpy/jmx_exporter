package io.prometheus.jmx.kafka;

import java.util.Properties;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class KafkaClientHolder extends AbstractKafkaService {

    private final AdminClient adminClient;

    public KafkaClientHolder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);

        Properties props = getProperties();
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaConfig.getRequestTimeoutMs());

        adminClient = AdminClient.create(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> adminClient.close()));
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    @Override
    public <T extends AbstractOptions> T timeoutMs(AbstractOptions<T> options) {
        return super.timeoutMs(options);
    }

    @Override
    public Properties getProperties() {
        return super.getProperties();
    }
}
