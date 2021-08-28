package com.pvoeten.kafkastreams.streams.billoflading;

import com.pvoeten.kafkastreams.billoflading.BillOfLading;
import com.pvoeten.kafkastreams.service.VesselVisitService;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@Scope("prototype") // Kafka needs a new instance everytime the
public class BillOfLadingTransformer implements ValueTransformer<BillOfLading, BillOfLadingProjection> {

    private TimestampedKeyValueStore<String, BillOfLading> billsOfLadingBuffer;
    private ProcessorContext context;
    private VesselVisitService vesselVisitService;

    public BillOfLadingTransformer(VesselVisitService vesselVisitService) {
        this.vesselVisitService = vesselVisitService;
    }

    @Override
    public void init(ProcessorContext context) {
        billsOfLadingBuffer = context.getStateStore(BillOfLadingStream.BILLS_OF_LADING_BUFFER);
        this.context = context;
    }

    @Override
    public BillOfLadingProjection transform(BillOfLading value) {
        VesselVisit vesselVisit = vesselVisitService.getVesselVisit(value.getVesselVisitId());
        final String key = value.getId();
        if (vesselVisit != null) {
            log.info("BL and VesselVisit could be matched!!! {} - {}", key, vesselVisit.getId());
            return BillOfLadingProjection.builder()
                .id(key)
                .dateRegistered(value.getDateRegistered())
                .vesselVisit(vesselVisit)
                .build();
        }
        // Vessel Visit not registered yet
        final long epochMilli = Instant.now().toEpochMilli();
        billsOfLadingBuffer.put(key, ValueAndTimestamp.make(value, epochMilli));
        log.info("Putting BillOfLading [{}] in buffer at: {}", key, Instant.ofEpochMilli(epochMilli));
        return null;
    }

    @Override
    public void close() {
        // do nothing
    }
}
