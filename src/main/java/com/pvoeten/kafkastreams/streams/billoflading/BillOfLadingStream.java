package com.pvoeten.kafkastreams.streams.billoflading;

import com.pvoeten.kafkastreams.streams.AbstractStream;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.stereotype.Component;

@Component
public class BillOfLadingStream extends AbstractStream {
    @Override
    public Topology topology() {
        var builder = new StreamsBuilder();

        builder.stream("bills-of-lading", Consumed.with(Serdes.String(), CustomSerdes.Json(BillOfLading.class)));

        return builder.build();
    }
}
