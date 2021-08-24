package com.pvoeten.kafkastreams.simulators;

import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class VesselVisitSimulator implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.replication-factor}")
    private String replicationFactor;

    @Value("${generate.vessel-visit.id-length}")
    private int idLength;

    @Value("$(generate.vessel-visit.interval)")
    private int interval;

    @Value("$(generate.vessel-visit.amount)")
    private int amount;

    private EventGateway gateway;

    @Override
    public void run(ApplicationArguments args) {
        gateway = gatewayBuilder().eventGateway();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::createVesselVisit, 30000, interval, TimeUnit.MILLISECONDS);
    }

    public GatewayBuilder gatewayBuilder() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new GatewayBuilder(properties);
    }

    private void createVesselVisit() {
        VesselVisit vesselVisit = VesselVisit.builder()
            .id(RandomStringUtils.randomAlphabetic(idLength))
            .vesselName(RandomStringUtils.randomAlphanumeric(20))
            .updated(Instant.now())
            .build();

        gateway.publish(vesselVisit);
        log.info("Simulator created new VesselVisit [{}]", vesselVisit);
    }
}
