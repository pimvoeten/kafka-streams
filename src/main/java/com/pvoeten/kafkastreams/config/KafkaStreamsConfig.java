package com.pvoeten.kafkastreams.config;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.Properties;

@Configuration
public class KafkaStreamsConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.replication-factor}")
    private String replicationFactor;

    @Value("${server.port}")
    private int port;

    @Value("${kafka.allow-topic-creation}")
    private boolean allowTopicCreation;

    @Value("${kafka.state-dir}")
    private String stateDir;

    @Bean
    @SneakyThrows
    public HostInfo hostInfo() {
        var host = InetAddress.getLocalHost().getHostAddress();
        return new HostInfo(host, port);
    }

    @Bean("kafkaProperties")
    public Properties properties(HostInfo hostInfo) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:%s", hostInfo.host(), hostInfo.port()));
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowTopicCreation);

        return properties;
    }
}
