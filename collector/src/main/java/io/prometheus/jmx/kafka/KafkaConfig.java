package io.prometheus.jmx.kafka;

public class KafkaConfig {
    private String bootServer;

    private int requestTimeoutMs;

    private String securityProtocol;

    private String saslMechanism;

    private String saslJaasConfig;

    private boolean enableAcl;

    public KafkaConfig() {}

    public KafkaConfig(
            String bootServer,
            int requestTimeoutMs,
            String securityProtocol,
            String saslMechanism,
            String saslJaasConfig,
            boolean enableAcl) {
        this.bootServer = bootServer;
        this.requestTimeoutMs = requestTimeoutMs;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
        this.saslJaasConfig = saslJaasConfig;
        this.enableAcl = enableAcl;
    }

    public String getBootServer() {
        return bootServer;
    }

    public void setBootServer(String bootServer) {
        this.bootServer = bootServer;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    public boolean isEnableAcl() {
        return enableAcl;
    }

    public void setEnableAcl(boolean enableAcl) {
        this.enableAcl = enableAcl;
    }
}
