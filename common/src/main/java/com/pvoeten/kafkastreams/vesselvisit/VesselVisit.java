package com.pvoeten.kafkastreams.vesselvisit;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
public class VesselVisit {
    String id;
    Instant updated;
    String vesselName;
}
