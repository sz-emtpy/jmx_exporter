package io.prometheus.jmx.kafka;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;

public abstract class AbstractKafkaService {
    public static final String INNER_CONSUMER = "__kafka-exporter-inner_consumer";

    private KafkaConfig kafkaConfig;

    public AbstractKafkaService(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    protected Properties getProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootServer());
        if (kafkaConfig.isEnableAcl()) {
            props.put(
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    kafkaConfig.getSecurityProtocol());
            props.put(SaslConfigs.SASL_MECHANISM, kafkaConfig.getSaslMechanism());
            props.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaConfig.getSaslJaasConfig());
        }

        return props;
    }

    protected <T extends AbstractOptions> T timeoutMs(AbstractOptions<T> options) {
        options.timeoutMs(kafkaConfig.getRequestTimeoutMs());
        return (T) options;
    }
}
