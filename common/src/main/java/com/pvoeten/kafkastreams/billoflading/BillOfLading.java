package com.pvoeten.kafkastreams.billoflading;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@TopicInfo("bills-of-lading")
@Value
@Builder(toBuilder = true)
public class BillOfLading {
    @AggregateId
    String id;
    Instant dateRegistered;
    String vesselVisitId;
}
