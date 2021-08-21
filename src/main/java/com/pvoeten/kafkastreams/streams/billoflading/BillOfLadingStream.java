package com.pvoeten.kafkastreams.streams.billoflading;

import com.pvoeten.kafkastreams.streams.AbstractStream;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class BillOfLadingStream extends AbstractStream implements ApplicationContextAware {

    public static final String BILLS_OF_LADING_BUFFER = "bills-of-lading-buffer";
    public static final String BILLS_OF_LADING_STORE = "bills-of-lading-store";
    private ApplicationContext applicationContext;

    @Override
    public Topology topology() {
        var builder = new StreamsBuilder();

        builder.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                    Stores.persistentTimestampedKeyValueStore(BILLS_OF_LADING_BUFFER),
                    Serdes.String(),
                    CustomSerdes.Json(BillOfLading.class)
                )
                .withLoggingDisabled()
//                .withLoggingEnabled(Collections.emptyMap())
        );

        builder.stream("bills-of-lading", Consumed.with(Serdes.String(), CustomSerdes.Json(BillOfLading.class)))
            .transform(() -> applicationContext.getBean(BillOfLadingTransformer.class), BILLS_OF_LADING_BUFFER)
            .filter((key, value) -> value != null)
            .toTable(
                Materialized.<String, BillOfLadingProjection>as(
                        Stores.persistentKeyValueStore(BILLS_OF_LADING_STORE))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(CustomSerdes.Json(BillOfLadingProjection.class))
            )
            .toStream()
            .to("matched.results", Produced.with(Serdes.String(), CustomSerdes.Json(BillOfLadingProjection.class)));

        return builder.build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
