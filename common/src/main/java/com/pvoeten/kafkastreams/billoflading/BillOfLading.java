package com.pvoeten.kafkastreams.billoflading;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
public class BillOfLading {
    String id;
    Instant dateRegistered;
    String vesselVisitId;
}
