package com.pvoeten.kafkastreams.simulators;

import com.pvoeten.kafkastreams.streams.billoflading.BillOfLading;
import io.github.alikelleci.easysourcing.GatewayBuilder;
import io.github.alikelleci.easysourcing.messages.events.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class BillOfLadingSimulator implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.replication-factor}")
    private String replicationFactor;

    private EventGateway gateway;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        gateway = gatewayBuilder().eventGateway();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::createBillOfLading, 10, 10, TimeUnit.SECONDS);
    }

    public GatewayBuilder gatewayBuilder() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new GatewayBuilder(properties);
    }

    private void createBillOfLading() {
        BillOfLading billOfLading = BillOfLading.builder()
            .id(UUID.randomUUID().toString())
            .dateRegistered(Instant.now())
            .vesselVisitId(RandomStringUtils.randomAlphanumeric(1))
            .build();

        gateway.publish(billOfLading);
        log.info("Simulator created new BillOfLading [{}]", billOfLading);
    }
}
