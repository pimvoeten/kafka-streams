package com.pvoeten.kafkastreams.streams.vesselvisit;

import com.pvoeten.kafkastreams.streams.AbstractStream;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
public class VesselVisitStream extends AbstractStream {
    @Override
    public Topology topology() {
        var builder = new StreamsBuilder();

        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore("vessel-visits"),
                    Serdes.String(),
                    CustomSerdes.Json(VesselVisit.class)
                )
                .withLoggingEnabled(Collections.emptyMap())
        );

        builder.stream("vessel-visit", Consumed.with(Serdes.String(), CustomSerdes.Json(VesselVisit.class)))
            .process(VesselVisitStateStoreProcessor::new, "vessel-visits");

        return builder.build();
    }
}
