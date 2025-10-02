package com.ibm.eventstreams.connect.rabbitmqsource.config;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Map;

class RabbitMQConnectorConfig extends AbstractConfig {

    public final String host;
    public static final String CONFIG_NAME_RABBITMQ_HOST = "rabbitmq.host";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_HOST = "The name of the the RabbitMQ host.";

    public final int port;
    public static final String CONFIG_NAME_RABBITMQ_PORT = "rabbitmq.port";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_PORT = "The port that RabbitMQ will listen on. ";

    public final String username;
    public static final String CONFIG_NAME_RABBITMQ_USERNAME = "rabbitmq.username";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_USERNAME = "The username for authenticating with RabbitMQ.";

    public final String password;
    public static final String CONFIG_NAME_RABBITMQ_PASSWORD = "rabbitmq.password";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_PASSWORD = "The password for authenticating with RabbitMQ.";

    public final String virtualHost;
    public static final String CONFIG_NAME_RABBITMQ_VIRTUAL_HOST = "rabbitmq.virtual.host";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_VIRTUAL_HOST = "The virtual host that RabbitMQ uses when connecting to the broker.";

    public final int requestedChannelMax;
    public static final String CONFIG_NAME_RABBITMQ_REQUESTED_CHANNEL_MAX = "rabbitmq.requested.channel.max";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_REQUESTED_CHANNEL_MAX = "The maximum number of channels that can be open on a connection simultaneously.";

    public final int requestedFrameMax;
    public static final String CONFIG_NAME_RABBITMQ_REQUESTED_FRAME_MAX = "rabbitmq.requested.frame.max";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_REQUESTED_FRAME_MAX = "The maximum frame size (in octets) allowed for the connection or zero for unlimited.";

    public final int connectionTimeout;
    public static final String CONFIG_NAME_RABBITMQ_CONNECTION_TIMEOUT = "rabbitmq.connection.timeout.ms";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_CONNECTION_TIMEOUT = "The connection timeout with a default of 60s and can be set zero for infinite.";

    public final int handshakeTimeout;
    public static final String CONFIG_NAME_RABBITMQ_HANDSHAKE_TIMEOUT = "rabbitmq.handshake.timeout.ms";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_HANDSHAKE_TIMEOUT = "The AMQP0-9-1 protocol handshake timeout with a default of 10 seconds.";

    public final int shutdownTimeout;
    public static final String CONFIG_NAME_RABBITMQ_SHUTDOWN_TIMEOUT = "rabbitmq.shutdown.timeout.ms";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SHUTDOWN_TIMEOUT = "The time after a connection is closed but before consumer is torn down.";

    public final int requestedHeartbeat;
    public static final String CONFIG_NAME_RABBITMQ_REQUESTED_HEARTBEAT = "rabbitmq.requested.heartbeat.seconds";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_REQUESTED_HEARTBEAT = "The heartbeat timeout value defines after what period of time the peer TCP connection should be considered unreachable.";

    public final boolean automaticRecoveryEnabled;
    public static final String CONFIG_NAME_RABBITMQ_AUTOMATIC_RECOVERY_ENABLED = "rabbitmq.automatic.recovery.enabled";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_AUTOMATIC_RECOVERY_ENABLED = "Enables or disables automatic connection recovery.";

    public final boolean topologyRecoveryEnabled;
    public static final String CONFIG_NAME_RABBITMQ_TOPOLOGY_RECOVERY_ENABLED = "rabbitmq.topology.recovery.enabled";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_TOPOLOGY_RECOVERY_ENABLED = "Enables or disables topology recovery.";

    public final long networkRecoveryInterval;
    public static final String CONFIG_NAME_RABBITMQ_NETWORK_RECOVERY_INTERVAL = "rabbitmq.network.recovery.interval.ms";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_NETWORK_RECOVERY_INTERVAL = "The time before a retry on an automatic recovery is performed (default 5 seconds).";

    public final ConnectionFactory connectionFactory;
    public final boolean sslEnabled;
    public static final String CONFIG_NAME_RABBITMQ_SSL_ENABLED = "rabbitmq.ssl.enabled";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_ENABLED = "Enable TLS/SSL when connecting to RabbitMQ.";

    public final String sslAlgorithm;
    public static final String CONFIG_NAME_RABBITMQ_SSL_ALGORITHM = "rabbitmq.ssl.algorithm";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_ALGORITHM = "TLS protocol algorithm used when SSL is enabled.";

    public final String sslTruststorePath;
    public static final String CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_PATH = "rabbitmq.ssl.truststore.path";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_TRUSTSTORE_PATH = "Path to the truststore used to validate the RabbitMQ server certificate.";

    public final String sslTruststorePassword;
    public static final String CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_PASSWORD = "rabbitmq.ssl.truststore.password";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_TRUSTSTORE_PASSWORD = "Password for the truststore when TLS is enabled.";

    public final String sslTruststoreType;
    public static final String CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_TYPE = "rabbitmq.ssl.truststore.type";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_TRUSTSTORE_TYPE = "Type of the truststore (for example JKS or PKCS12).";

    public final String sslKeystorePath;
    public static final String CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_PATH = "rabbitmq.ssl.keystore.path";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_KEYSTORE_PATH = "Optional path to the keystore containing the client certificate for mutual TLS.";

    public final String sslKeystorePassword;
    public static final String CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_PASSWORD = "rabbitmq.ssl.keystore.password";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_KEYSTORE_PASSWORD = "Password protecting the optional keystore when TLS is enabled.";

    public final String sslKeystoreType;
    public static final String CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_TYPE = "rabbitmq.ssl.keystore.type";
    public static final String CONFIG_DOCUMENTATION_RABBITMQ_SSL_KEYSTORE_TYPE = "Type of the optional keystore (for example JKS or PKCS12).";

    public final SSLContext sslContext;


    public RabbitMQConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);

        this.host = this.getString(CONFIG_NAME_RABBITMQ_HOST);
        this.port = this.getInt(CONFIG_NAME_RABBITMQ_PORT);
        this.username = this.getString(CONFIG_NAME_RABBITMQ_USERNAME);
        this.password = this.getString(CONFIG_NAME_RABBITMQ_PASSWORD);
        this.virtualHost = this.getString(CONFIG_NAME_RABBITMQ_VIRTUAL_HOST);
        this.requestedChannelMax = this.getInt(CONFIG_NAME_RABBITMQ_REQUESTED_CHANNEL_MAX);
        this.requestedFrameMax = this.getInt(CONFIG_NAME_RABBITMQ_REQUESTED_FRAME_MAX);
        this.connectionTimeout = this.getInt(CONFIG_NAME_RABBITMQ_CONNECTION_TIMEOUT);
        this.handshakeTimeout = this.getInt(CONFIG_NAME_RABBITMQ_HANDSHAKE_TIMEOUT);
        this.shutdownTimeout = this.getInt(CONFIG_NAME_RABBITMQ_SHUTDOWN_TIMEOUT);
        this.requestedHeartbeat = this.getInt(CONFIG_NAME_RABBITMQ_REQUESTED_HEARTBEAT);
        this.automaticRecoveryEnabled = this.getBoolean(CONFIG_NAME_RABBITMQ_AUTOMATIC_RECOVERY_ENABLED);
        this.topologyRecoveryEnabled = this.getBoolean(CONFIG_NAME_RABBITMQ_TOPOLOGY_RECOVERY_ENABLED);
        this.networkRecoveryInterval = this.getInt(CONFIG_NAME_RABBITMQ_NETWORK_RECOVERY_INTERVAL);
        this.sslEnabled = this.getBoolean(CONFIG_NAME_RABBITMQ_SSL_ENABLED);
        this.sslAlgorithm = this.getString(CONFIG_NAME_RABBITMQ_SSL_ALGORITHM);
        this.sslTruststorePath = this.getString(CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_PATH);
        this.sslTruststorePassword = passwordValue(this.getPassword(CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_PASSWORD));
        this.sslTruststoreType = this.getString(CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_TYPE);
        this.sslKeystorePath = this.getString(CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_PATH);
        this.sslKeystorePassword = passwordValue(this.getPassword(CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_PASSWORD));
        this.sslKeystoreType = this.getString(CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_TYPE);
        this.sslContext = this.sslEnabled ? buildSslContext() : null;

        this.connectionFactory = connectionFactory();
    }

    public static ConfigDef config() {
        ConfigDef config = new ConfigDef();

        config.define(CONFIG_NAME_RABBITMQ_HOST, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_HOST, ConfigDef.Importance.HIGH, CONFIG_DOCUMENTATION_RABBITMQ_HOST);
        config.define(CONFIG_NAME_RABBITMQ_PORT, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_AMQP_PORT, ConfigDef.Importance.MEDIUM, CONFIG_DOCUMENTATION_RABBITMQ_PORT);
        config.define(CONFIG_NAME_RABBITMQ_USERNAME, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_USER, ConfigDef.Importance.HIGH, CONFIG_DOCUMENTATION_RABBITMQ_USERNAME);
        config.define(CONFIG_NAME_RABBITMQ_PASSWORD, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_PASS, ConfigDef.Importance.HIGH, CONFIG_DOCUMENTATION_RABBITMQ_PASSWORD);
        config.define(CONFIG_NAME_RABBITMQ_VIRTUAL_HOST, ConfigDef.Type.STRING, ConnectionFactory.DEFAULT_VHOST, ConfigDef.Importance.HIGH, CONFIG_DOCUMENTATION_RABBITMQ_VIRTUAL_HOST);
        config.define(CONFIG_NAME_RABBITMQ_REQUESTED_CHANNEL_MAX, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_CHANNEL_MAX, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_REQUESTED_CHANNEL_MAX);
        config.define(CONFIG_NAME_RABBITMQ_REQUESTED_FRAME_MAX, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_FRAME_MAX, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_REQUESTED_FRAME_MAX);
        config.define(CONFIG_NAME_RABBITMQ_CONNECTION_TIMEOUT, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_CONNECTION_TIMEOUT);
        config.define(CONFIG_NAME_RABBITMQ_HANDSHAKE_TIMEOUT, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_HANDSHAKE_TIMEOUT, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_HANDSHAKE_TIMEOUT);
        config.define(CONFIG_NAME_RABBITMQ_SHUTDOWN_TIMEOUT, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SHUTDOWN_TIMEOUT);
        config.define(CONFIG_NAME_RABBITMQ_REQUESTED_HEARTBEAT, ConfigDef.Type.INT, ConnectionFactory.DEFAULT_HEARTBEAT, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_REQUESTED_HEARTBEAT);
        config.define(CONFIG_NAME_RABBITMQ_AUTOMATIC_RECOVERY_ENABLED, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_AUTOMATIC_RECOVERY_ENABLED);
        config.define(CONFIG_NAME_RABBITMQ_TOPOLOGY_RECOVERY_ENABLED, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_TOPOLOGY_RECOVERY_ENABLED);
        config.define(CONFIG_NAME_RABBITMQ_NETWORK_RECOVERY_INTERVAL, ConfigDef.Type.INT, 10000, ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_NETWORK_RECOVERY_INTERVAL);
        config.define(CONFIG_NAME_RABBITMQ_SSL_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, CONFIG_DOCUMENTATION_RABBITMQ_SSL_ENABLED);
        config.define(CONFIG_NAME_RABBITMQ_SSL_ALGORITHM, ConfigDef.Type.STRING, "TLSv1.2", ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SSL_ALGORITHM);
        config.define(CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_PATH, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, CONFIG_DOCUMENTATION_RABBITMQ_SSL_TRUSTSTORE_PATH);
        config.define(CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SSL_TRUSTSTORE_PASSWORD);
        config.define(CONFIG_NAME_RABBITMQ_SSL_TRUSTSTORE_TYPE, ConfigDef.Type.STRING, KeyStore.getDefaultType(), ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SSL_TRUSTSTORE_TYPE);
        config.define(CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_PATH, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SSL_KEYSTORE_PATH);
        config.define(CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_PASSWORD, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SSL_KEYSTORE_PASSWORD);
        config.define(CONFIG_NAME_RABBITMQ_SSL_KEYSTORE_TYPE, ConfigDef.Type.STRING, KeyStore.getDefaultType(), ConfigDef.Importance.LOW, CONFIG_DOCUMENTATION_RABBITMQ_SSL_KEYSTORE_TYPE);

        return config;
    }

    public final ConnectionFactory connectionFactory() {
        ConnectionFactory connectionFactory = new ConnectionFactory();

        connectionFactory.setHost(this.host);
        connectionFactory.setPort(this.port);
        connectionFactory.setUsername(this.username);
        connectionFactory.setPassword(this.password);
        connectionFactory.setVirtualHost(this.virtualHost);
        connectionFactory.setRequestedChannelMax(this.requestedChannelMax);
        connectionFactory.setRequestedFrameMax(this.requestedFrameMax);
        connectionFactory.setConnectionTimeout(this.connectionTimeout);
        connectionFactory.setHandshakeTimeout(this.handshakeTimeout);
        connectionFactory.setShutdownTimeout(this.shutdownTimeout);
        connectionFactory.setRequestedHeartbeat(this.requestedHeartbeat);
        connectionFactory.setAutomaticRecoveryEnabled(this.automaticRecoveryEnabled);
        connectionFactory.setTopologyRecoveryEnabled(this.topologyRecoveryEnabled);
        connectionFactory.setNetworkRecoveryInterval(this.networkRecoveryInterval);

        if (this.sslEnabled) {
            try {
                if (this.sslContext != null) {
                    connectionFactory.useSslProtocol(this.sslContext);
                } else {
                    connectionFactory.useSslProtocol();
                }
            } catch (GeneralSecurityException e) {
                throw new ConfigException("Failed to enable TLS for RabbitMQ connection", e);
            }
        }

        return connectionFactory;
    }

    private SSLContext buildSslContext() {
        try {
            String algorithm = defaultIfBlank(this.sslAlgorithm, "TLSv1.2");
            SSLContext context = SSLContext.getInstance(algorithm);

            KeyManagerFactory keyManagerFactory = null;
            if (!this.sslKeystorePath.isEmpty()) {
                KeyStore keyStore = loadKeyStore(defaultIfBlank(this.sslKeystoreType, KeyStore.getDefaultType()), this.sslKeystorePath, this.sslKeystorePassword);
                keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, passwordChars(this.sslKeystorePassword));
            }

            TrustManagerFactory trustManagerFactory = null;
            if (!this.sslTruststorePath.isEmpty()) {
                KeyStore trustStore = loadKeyStore(defaultIfBlank(this.sslTruststoreType, KeyStore.getDefaultType()), this.sslTruststorePath, this.sslTruststorePassword);
                trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
            }

            context.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null,
                    trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null,
                    null);

            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new ConfigException("Failed to load SSL configuration", e);
        }
    }

    private static KeyStore loadKeyStore(String type, String path, String password) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(defaultIfBlank(type, KeyStore.getDefaultType()));
        try (FileInputStream inputStream = new FileInputStream(path)) {
            keyStore.load(inputStream, passwordArray(password));
        }
        return keyStore;
    }

    private static char[] passwordArray(String password) {
        if (password == null || password.isEmpty()) {
            return null;
        }
        return password.toCharArray();
    }

    private static char[] passwordChars(String password) {
        if (password == null) {
            return new char[0];
        }
        return password.toCharArray();
    }

    private static String passwordValue(Password password) {
        return password == null ? "" : password.value();
    }

    private static String defaultIfBlank(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }

}
