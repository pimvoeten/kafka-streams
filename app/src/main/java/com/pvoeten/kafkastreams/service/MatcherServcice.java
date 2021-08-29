package com.pvoeten.kafkastreams.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pvoeten.kafkastreams.billoflading.BillOfLading;
import com.pvoeten.kafkastreams.streams.billoflading.BillOfLadingProjection;
import com.pvoeten.kafkastreams.streams.billoflading.BillOfLadingStream;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
//@Service
public class MatcherServcice implements ApplicationRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private BillOfLadingStream billOfLadingStream;
    private VesselVisitService vesselVisitService;
    private ObjectMapper objectMapper;
    private KafkaProducer<String, BillOfLadingProjection> kafkaProducer;

    public MatcherServcice(BillOfLadingStream billOfLadingStream, VesselVisitService vesselVisitService, ObjectMapper objectMapper) {
        this.billOfLadingStream = billOfLadingStream;
        this.vesselVisitService = vesselVisitService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, this.getClass().getSimpleName() + "-" + UUID.randomUUID());
        kafkaProducer = new KafkaProducer<>(config, Serdes.String().serializer(), new JsonSerializer<>(objectMapper));
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(this::punctuate, 30, 30, TimeUnit.SECONDS);
    }

    public void punctuate() {
        log.info("Start punctuation at: {}", Instant.now());
        final KafkaStreams billOfLadingStreamKafkaStream = billOfLadingStream.getKafkaStream();
        if (billOfLadingStreamKafkaStream.state() != KafkaStreams.State.RUNNING) {
            return;
        }

        final ReadOnlyKeyValueStore<String, BillOfLading> blStore = billOfLadingStreamKafkaStream.store(
            StoreQueryParameters.fromNameAndType(BillOfLadingStream.BILLS_OF_LADING_STORE, QueryableStoreTypes.keyValueStore())
        );
        try (KeyValueIterator<String, BillOfLading> billOfLadingKeyValueIterator = blStore.all()) {
            while (billOfLadingKeyValueIterator.hasNext()) {
                final KeyValue<String, BillOfLading> billOfLadingKeyValue = billOfLadingKeyValueIterator.next();
                final BillOfLading billOfLading = billOfLadingKeyValue.value;
                final String key = billOfLadingKeyValue.key;

                VesselVisit vesselVisit = vesselVisitService.getVesselVisit(billOfLading.getVesselVisitId());
                if (vesselVisit != null) {
                    log.info("Vessel visit found [{}]", vesselVisit);
                    log.info("BL [{}] and vessel visit [{}] can be matched", key, billOfLading.getVesselVisitId());

                    log.info("Removing BillOfLading [{}] from buffer at: {}", key, Instant.now());
                    final BillOfLadingProjection billOfLadingProjection = BillOfLadingProjection.builder()
                        .id(key)
                        .dateRegistered(billOfLading.getDateRegistered())
                        .vesselVisit(vesselVisit)
                        .build();
                    kafkaProducer.send(new ProducerRecord<>("matched.results", billOfLadingProjection));
                }
                log.info("BL [{}] still has no registered vessel visit [{}]", key, billOfLading.getVesselVisitId());
            }
        } catch (Exception e) {
            log.error("", e);
        }
        log.info("End punctuation at: {}", Instant.now());
    }
}
