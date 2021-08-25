package com.pvoeten.kafkastreams.streams.billoflading;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pvoeten.kafkastreams.config.ObjectMapperConfig;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class BillOfLadingProjectionTest {

    ObjectMapperConfig springConfig = new ObjectMapperConfig();

    @Test
    public void testToJson() throws JsonProcessingException {
        final BillOfLadingProjection billOfLadingProjection = BillOfLadingProjection.builder()
            .dateRegistered(Instant.now())
            .id("1")
            .vesselVisit(
                VesselVisit.builder()
                    .id("vv")
                    .updated(Instant.now())
                    .vesselName("shippie")
                    .build()
            )
            .build();

        final String json = springConfig.objectMapper().writeValueAsString(billOfLadingProjection);
        System.out.println(json);
    }
}