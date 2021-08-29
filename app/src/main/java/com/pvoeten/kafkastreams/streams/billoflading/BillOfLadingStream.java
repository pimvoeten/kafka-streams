package com.pvoeten.kafkastreams.streams.billoflading;

import com.pvoeten.kafkastreams.billoflading.BillOfLading;
import com.pvoeten.kafkastreams.streams.AbstractStream;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
public class BillOfLadingStream extends AbstractStream implements ApplicationContextAware {

    public static final String BILLS_OF_LADING_STORE = "bills-of-lading-store";
    private ApplicationContext applicationContext;

    @Override
    public Topology topology() {
        var builder = new StreamsBuilder();

        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                    Stores.persistentTimestampedKeyValueStore(BILLS_OF_LADING_STORE),
                    Serdes.String(),
                    CustomSerdes.Json(BillOfLadingProjection.class)
                )
                .withLoggingEnabled(Collections.emptyMap())
        );

        KTable<String, VesselVisit> vesselVisits = builder
            .table("vessel-visits", Consumed.with(Serdes.String(), CustomSerdes.Json(VesselVisit.class)));

        builder.stream("bills-of-lading", Consumed.with(Serdes.String(), CustomSerdes.Json(BillOfLading.class)))
            .selectKey((key, value) -> value.getVesselVisitId())
            .toTable(Materialized.with(Serdes.String(), CustomSerdes.Json(BillOfLading.class)))
            .join(vesselVisits, (bl, vv) -> {
                if (bl.getVesselVisitId().equals(vv.getId())) {
                    return BillOfLadingProjection.builder()
                        .id(bl.getId())
                        .dateRegistered(bl.getDateRegistered())
                        .vesselVisit(vv)
                        .build();
                }
                return null;
            })
            .toStream()
            .filter((key, value) -> value != null)
            .peek((key, value) -> log.info("{}: {}", key, value))
            .selectKey((key, value) -> value.getId())
            .to("matched.results", Produced.with(Serdes.String(), CustomSerdes.Json(BillOfLadingProjection.class)));

        builder.stream("matched.results", Consumed.with(Serdes.String(), CustomSerdes.Json(BillOfLadingProjection.class)))
            .process(MatchedProcessor::new, BILLS_OF_LADING_STORE);
        return builder.build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
