package com.pvoeten.kafkastreams.simulators;

import com.pvoeten.kafkastreams.billoflading.BillOfLading;
import io.github.alikelleci.easysourcing.GatewayBuilder;
import io.github.alikelleci.easysourcing.messages.events.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
@Component
public class BillOfLadingSimulator implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("$(generate.bill-of-lading.interval)")
    private int interval;

    @Value("$(generate.bill-of-lading.amount)")
    private int amount;

    @Value("${app.vessel-visit.id-length}")
    private int idLength;

    private EventGateway gateway;

    @Override
    public void run(ApplicationArguments args) {
        gateway = gatewayBuilder().eventGateway();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::generate, 30000, interval, TimeUnit.MILLISECONDS);
    }

    public GatewayBuilder gatewayBuilder() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new GatewayBuilder(properties);
    }

    // TODO: publish in batches
    private void generate() {
        IntStream.range(0, amount)
            .forEach(i -> {
                    BillOfLading billOfLading = BillOfLading.builder()
                        .id(UUID.randomUUID().toString())
                        .dateRegistered(Instant.now())
                        .vesselVisitId(RandomStringUtils.randomAlphabetic(idLength))
                        .build();

                    gateway.publish(billOfLading);
                }
            );
    }
}
