package com.pvoeten.kafkastreams.streams.billoflading;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@TopicInfo("bill-of-lading")
@Value
@Builder(toBuilder = true)
public class BillOfLading {
    @AggregateId
    private String id;
    private Instant dateRegistered;
    private String vesselVisitId;
}
