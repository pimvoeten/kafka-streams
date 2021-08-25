package com.pvoeten.kafkastreams.generators;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
@Component
public class VesselVisitGenerator implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${generate.vessel-visit.id-length}")
    private int idLength;

    @Value("${generate.vessel-visit.interval}")
    private int interval;

    @Value("${generate.vessel-visit.amount}")
    private int amount;

    private KafkaProducer<String, VesselVisit> producer;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void run(ApplicationArguments args) {
        log.info("Starting {} with VV id length: {}, interval: {}, amount: {}", this.getClass().getSimpleName(), idLength, interval, amount);

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "VesselVisitGenerator-" + UUID.randomUUID());
        producer = new KafkaProducer<>(config, Serdes.String().serializer(), new JsonSerializer<>(objectMapper));

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::generate, 30000, interval, TimeUnit.MILLISECONDS);
    }

    private void generate() {
        IntStream.range(0, amount)
            .forEach(i -> {
                    VesselVisit vesselVisit = VesselVisit.builder()
                        .id(RandomStringUtils.randomAlphabetic(idLength))
                        .vesselName(RandomStringUtils.randomAlphanumeric(20))
                        .updated(Instant.now())
                        .build();

                    producer.send(new ProducerRecord<>("vessel-visits", vesselVisit.getId(), vesselVisit));
                }
            );
        producer.flush();
    }
}
