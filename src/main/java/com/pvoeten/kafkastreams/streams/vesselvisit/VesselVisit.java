package com.pvoeten.kafkastreams.streams.vesselvisit;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@TopicInfo("vessel-visit")
@Value
@Builder(toBuilder = true)
public class VesselVisit {
    @AggregateId
    private String id;
    private Instant updated;
    private String vesselName;
}
