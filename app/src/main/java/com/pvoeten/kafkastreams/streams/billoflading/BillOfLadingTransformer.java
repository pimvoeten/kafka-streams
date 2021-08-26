package com.pvoeten.kafkastreams.streams.billoflading;

import com.pvoeten.kafkastreams.billoflading.BillOfLading;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.context.annotation.Scope;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@Scope("prototype") // Kafka needs a new instance everytime the
public class BillOfLadingTransformer implements Transformer<String, BillOfLading, KeyValue<String, BillOfLadingProjection>> {

    private final RestTemplate restTemplate;
    private final HostInfo hostInfo;

    private TimestampedKeyValueStore<String, BillOfLading> billsOfLadingBuffer;
    private ProcessorContext context;

    public BillOfLadingTransformer(RestTemplate restTemplate, HostInfo hostInfo) {
        this.restTemplate = restTemplate;
        this.hostInfo = hostInfo;
    }

    @Override
    public void init(ProcessorContext context) {
        billsOfLadingBuffer = context.getStateStore(BillOfLadingStream.BILLS_OF_LADING_BUFFER);
        this.context = context;

        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public KeyValue<String, BillOfLadingProjection> transform(String key, BillOfLading value) {
        VesselVisit vesselVisit = getVesselVisit(value.getVesselVisitId());
        if (vesselVisit != null) {
            log.info(">>>>> BL and VesselVisit could be matched!!! {} - {}", key, vesselVisit.getId());
            return KeyValue.pair(key, BillOfLadingProjection.builder()
                .id(value.getId())
                .dateRegistered(value.getDateRegistered())
                .vesselVisit(vesselVisit)
                .build());
        }
        // Vessel Visit not registered yet
        long timestamp = context.timestamp(); // TODO: sometimes equals to 1970-01-01T00:00:00Z
        final long epochMilli = Instant.now().toEpochMilli();
        billsOfLadingBuffer.put(key, ValueAndTimestamp.make(value, epochMilli));
        log.info("Putting BillOfLading [{}] in buffer at: {}", key, Instant.ofEpochMilli(epochMilli));
        return null;
    }

    private void punctuate(long punctuationTimestamp) {
        log.info(">>>>>>>>>> Start punctuation at: {}", Instant.ofEpochMilli(punctuationTimestamp));
        log.info(">>>>>>>>>> BL buffer contains: {}", billsOfLadingBuffer.approximateNumEntries());

        if (!billsOfLadingBuffer.isOpen()) {
            log.warn("Buffer store is not open yet");
        }
        try (KeyValueIterator<String, ValueAndTimestamp<BillOfLading>> keyValueIterator = billsOfLadingBuffer.all()) {
            while (keyValueIterator.hasNext()) {
                final KeyValue<String, ValueAndTimestamp<BillOfLading>> buffered = keyValueIterator.next();

                String key = buffered.key;
                ValueAndTimestamp<BillOfLading> value = buffered.value;
                long recordTimestamp = value.timestamp();
                BillOfLading billOfLading = value.value();

                log.info(">>>>>>>>>> BL [{}] has been buffered since {}", key, Instant.ofEpochMilli(recordTimestamp));
                VesselVisit vesselVisit = getVesselVisit(billOfLading.getVesselVisitId());
                if (vesselVisit == null) {
                    log.info(">>>>>>>>>> BL [{}] still has no registered vessel visit [{}]", key, billOfLading.getVesselVisitId());
                    continue;
                }
                log.info(">>>>>>>>>> Vessel visit found [{}]", vesselVisit);
                log.info(">>>>>>>>>> BL [{}] and vessel visit [{}] can be matched", key, billOfLading.getVesselVisitId());

                log.info("Removing BillOfLading [{}] from buffer at: {}", key, Instant.now());
                billsOfLadingBuffer.delete(key);

                context.forward(
                    key,
                    BillOfLadingProjection.builder()
                        .id(key)
                        .dateRegistered(billOfLading.getDateRegistered())
                        .vesselVisit(vesselVisit)
                        .build(),
                    To.all()
                );
                context.commit();
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            log.info("<<<<<<<<<< End of punctuation at: {}", Instant.now());
        }
    }

    private VesselVisit getVesselVisit(String vesselVisitId) {
        try {
            ResponseEntity<VesselVisit> response = restTemplate.getForEntity(
                String.format("http://%s:%s/api/vesselvisits/%s", "localhost", hostInfo.port(), vesselVisitId),
                VesselVisit.class);
            if (response.getStatusCode().isError()) {
                return null;
            }
            return response.getBody();
        } catch (HttpClientErrorException e) {
            return null;
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
