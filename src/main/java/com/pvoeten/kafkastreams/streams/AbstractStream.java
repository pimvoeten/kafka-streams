package com.pvoeten.kafkastreams.streams;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public abstract class AbstractStream implements ApplicationRunner {

    @Autowired
    @Qualifier("kafkaProperties")
    private Properties properties;

    @Getter
    private KafkaStreams kafkaStream;
    private ScheduledExecutorService executorService;

    public abstract Topology topology();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        kafkaStream = new KafkaStreams(topology(), properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStream::close));
        kafkaStream.start();

        executorService = Executors.newSingleThreadScheduledExecutor();
//        executorService.scheduleAtFixedRate(this::printMetrics, 60, 60, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void close() {
        executorService.shutdown();
    }

    private void printMetrics() {
        log.debug(this.kafkaStream.metrics().toString());
    }
}
