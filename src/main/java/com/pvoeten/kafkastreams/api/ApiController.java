package com.pvoeten.kafkastreams.api;

import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisitStream;
import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@Slf4j
@RestController
@RequestMapping(path = "/api")
public class ApiController {

    private ReadOnlyKeyValueStore<String, VesselVisit> vesselVisitsStore;
    private VesselVisitStream vesselVisitStream;

    public ApiController(VesselVisitStream vesselVisitStream) {
        this.vesselVisitStream = vesselVisitStream;
    }

    @GetMapping(path = "/vesselvisits")
    public KeyValueIterator<String, VesselVisit> getVesselVisits() {
        return vesselVisitsStore.all();
    }

    @GetMapping(path = "/vesselvisits/{id}")
    public ResponseEntity<VesselVisit> getVesselVisitById(@PathVariable("id") String id) {
        KeyQueryMetadata keyQueryMetadata = vesselVisitStream.getKafkaStream().queryMetadataForKey("vessel-visits", id, Serdes.String().serializer());
        if (keyQueryMetadata == null) {
            return ResponseEntity.notFound().build();
        }
        log.info("VesselVisit with id [{}] exists on partition {}", id, keyQueryMetadata.partition());
        return ResponseEntity.of(Optional.ofNullable(getStore().get(id)));
    }

    private ReadOnlyKeyValueStore<String, VesselVisit> getStore() {
        if (vesselVisitsStore == null) {
            vesselVisitsStore = vesselVisitStream.getKafkaStream()
                .store(
                    StoreQueryParameters.fromNameAndType(
                        "vessel-visits",
                        QueryableStoreTypes.keyValueStore()
                    )
                );
        }
        return vesselVisitsStore;
    }
}
