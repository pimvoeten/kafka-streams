package com.pvoeten.kafkastreams.api;

import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisit;
import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisitStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
public class ApiController {

    private ReadOnlyKeyValueStore<String, VesselVisit> vesselVisitsStore;
    private VesselVisitStream vesselVisitStream;
    private HostInfo hostInfo;
    private RestTemplate restTemplate;

    public ApiController(VesselVisitStream vesselVisitStream, HostInfo hostInfo, RestTemplate restTemplate) {
        this.vesselVisitStream = vesselVisitStream;
        this.hostInfo = hostInfo;
        this.restTemplate = restTemplate;
    }

    @GetMapping(path = "/vesselvisits")
    public List<String> getVesselVisits() {
        Collection<StreamsMetadata> streamsMetadata = vesselVisitStream.getKafkaStream().allMetadataForStore(VesselVisitStream.VESSEL_VISITS_STORE);
        return streamsMetadata.stream().map(StreamsMetadata::toString).collect(Collectors.toList());
    }

    @GetMapping(path = "/vesselvisits/{id}")
    public ResponseEntity<VesselVisit> getVesselVisitById(@PathVariable("id") String id) {
        KeyQueryMetadata keyQueryMetadata = vesselVisitStream.getKafkaStream()
            .queryMetadataForKey(VesselVisitStream.VESSEL_VISITS_STORE, id, Serdes.String().serializer());
        if (keyQueryMetadata == null) {
            log.info("VesselVisit with id [{}] does not exist", id);
            return ResponseEntity.notFound().build();
        }
        log.info("VesselVisit with id [{}] exists on partition {}", id, keyQueryMetadata.partition());
        HostInfo activeHost = keyQueryMetadata.activeHost();
        if (hostInfo.equals(activeHost)) {
            log.info("VesselVisit with id [{}] is here!!", id);
            return ResponseEntity.ok(getStore().get(id));
        }
        log.info("VesselVisit with id [{}] might exist on host {}", id, activeHost);

        String url = String.format("http://%s:%s/api/vesselvisits/%s", activeHost.host(), activeHost.port(), id);
        log.info("Retrieving vesselvisit with id [{}] from {}", id, url);
        VesselVisit vesselVisit = restTemplate.getForObject(url,
            VesselVisit.class);
        if (vesselVisit == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(vesselVisit);
    }

    private ReadOnlyKeyValueStore<String, VesselVisit> getStore() {
        if (vesselVisitsStore == null) {
            vesselVisitsStore = vesselVisitStream.getKafkaStream()
                .store(
                    StoreQueryParameters.fromNameAndType(
                        VesselVisitStream.VESSEL_VISITS_STORE,
                        QueryableStoreTypes.keyValueStore()
                    )
                );
        }
        return vesselVisitsStore;
    }
}
