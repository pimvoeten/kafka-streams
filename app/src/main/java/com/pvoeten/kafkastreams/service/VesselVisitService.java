package com.pvoeten.kafkastreams.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Properties;
import java.util.UUID;

@Service
public class VesselVisitService implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final ObjectMapper objectMapper;
    private RestTemplate restTemplate;
    private HostInfo hostInfo;

    private KafkaProducer<String, VesselVisit> producer;

    public VesselVisitService(ObjectMapper objectMapper, RestTemplate restTemplate, HostInfo hostInfo) {
        this.objectMapper = objectMapper;
        this.restTemplate = restTemplate;
        this.hostInfo = hostInfo;
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

    public VesselVisit getVesselVisit(String vesselVisitId) {
        try {
            ResponseEntity<VesselVisit> response = restTemplate.getForEntity(
                String.format("http://%s:%s/api/vesselvisits/%s", "localhost", hostInfo.port(), vesselVisitId),
                VesselVisit.class);
            if (response.getStatusCode().isError() || !response.hasBody()) {
                return null;
            }
            return response.getBody();
        } catch (RestClientException e) {
            return null;
        }
    }
}
