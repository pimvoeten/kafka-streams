package com.pvoeten.kafkastreams.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.UUID;

@Service
public class VesselVisitService implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ObjectMapper objectMapper;

    private KafkaProducer<String, VesselVisit> producer;

    public VesselVisitService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "VesselVisitGenerator-" + UUID.randomUUID());
        producer = new KafkaProducer<>(config, Serdes.String().serializer(), new JsonSerializer<>(objectMapper));
    }

    public void publish(VesselVisit vesselVisit) {
        producer.send(new ProducerRecord<>("vessel-visits", vesselVisit.getId(), vesselVisit));
        producer.flush();
    }
}
