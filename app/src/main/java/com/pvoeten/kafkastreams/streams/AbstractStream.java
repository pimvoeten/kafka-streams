package com.pvoeten.kafkastreams.streams;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public abstract class AbstractStream implements ApplicationRunner {

    @Value("${spring.application.name}")
    private String applicationName;

    @Autowired
    @Qualifier("kafkaProperties")
    private Properties properties;

    @Getter
    private KafkaStreams kafkaStream;
    private ScheduledExecutorService executorService;

    public abstract Topology topology();

    @Override
    public void run(ApplicationArguments args) {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, createApplicationId());
        kafkaStream = new KafkaStreams(topology(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStream::close));
        kafkaStream.setStateListener((newState, oldState) -> log.info("{} state changed from {} to {}", this.getClass().getSimpleName(), oldState, newState));
        kafkaStream.setUncaughtExceptionHandler((thread, throwable) -> log.error("", throwable));
        kafkaStream.cleanUp();
        kafkaStream.start();
    }

    @PreDestroy
    public void close() {
        executorService.shutdown();
    }

    private String createApplicationId() {
        return applicationName.concat("-").concat(this.getClass().getSimpleName());
    }
}
