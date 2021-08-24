package com.pvoeten.kafkastreams.vesselvisit;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@TopicInfo("vessel-visits")
@Value
@Builder(toBuilder = true)
public class VesselVisit {
    @AggregateId
    String id;
    Instant updated;
    String vesselName;
}
