package com.pvoeten.kafkastreams.streams.vesselvisit;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class VesselVisitStateStoreProcessor implements Processor<String, VesselVisit> {
    private KeyValueStore<String, VesselVisit> vesselVisitStore;

    @Override
    public void init(ProcessorContext context) {
        this.vesselVisitStore = context.getStateStore(VesselVisitStream.VESSEL_VISITS_STORE);
    }

    @Override
    public void process(String key, VesselVisit value) {
        vesselVisitStore.putIfAbsent(key, value);
    }

    @Override
    public void close() {
        // do nothing
    }
}
