package com.pvoeten.kafkastreams.streams.billoflading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
public class BillOfLadingProjection {
    String id;
    @JsonProperty("date-registered")
    Instant dateRegistered;
    @JsonProperty("vessel-visit")
    VesselVisit vesselVisit;
}