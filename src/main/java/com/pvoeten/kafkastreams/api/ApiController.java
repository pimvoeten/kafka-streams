package com.pvoeten.kafkastreams.api;

import com.pvoeten.kafkastreams.streams.billoflading.BillOfLadingProjection;
import com.pvoeten.kafkastreams.streams.billoflading.BillOfLadingStream;
import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisit;
import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisitStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
public class ApiController {

    private VesselVisitStream vesselVisitStream;
    private BillOfLadingStream billOfLadingStream;
    private HostInfo hostInfo;
    private RestTemplate restTemplate;
    private ReadOnlyKeyValueStore<String, BillOfLadingProjection> billOfLadingStore;
    private ReadOnlyKeyValueStore<String, VesselVisit> vesselVisitsStore;

    public ApiController(VesselVisitStream vesselVisitStream, BillOfLadingStream billOfLadingStream, HostInfo hostInfo, RestTemplate restTemplate) {
        this.vesselVisitStream = vesselVisitStream;
        this.billOfLadingStream = billOfLadingStream;
        this.hostInfo = hostInfo;
        this.restTemplate = restTemplate;
    }

    @GetMapping(path = "/metadata")
    public List<String> getMetadata() {
        Collection<StreamsMetadata> streamsMetadata = vesselVisitStream.getKafkaStream().allMetadataForStore(VesselVisitStream.VESSEL_VISITS_STORE);
        return streamsMetadata.stream().map(StreamsMetadata::toString).collect(Collectors.toList());
    }

    @GetMapping(path = "/bills-of-lading")
    public List<BillOfLadingProjection> getBillsOfLading() {
        // TODO: this will only return the local storage contents
        log.info("Bills of Lading count: {}", getBillOfLadingStore().approximateNumEntries());
        List<BillOfLadingProjection> response = new ArrayList<>();
        final KeyValueIterator<String, BillOfLadingProjection> iterator = getBillOfLadingStore().all();
        while (iterator.hasNext()) {
            final KeyValue<String, BillOfLadingProjection> bl = iterator.next();
            response.add(bl.value);
        }
        return response;
    }

    @GetMapping(path = "/bills-of-lading/{id}")
    public ResponseEntity<BillOfLadingProjection> getBillsOfLadingById(@PathVariable("id") String id) {
        return ResponseEntity.noContent().build();
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
            return ResponseEntity.ok(getVesselVisitStore().get(id));
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

    private ReadOnlyKeyValueStore<String, VesselVisit> getVesselVisitStore() {
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

    private ReadOnlyKeyValueStore<String, BillOfLadingProjection> getBillOfLadingStore() {
        if (billOfLadingStore == null) {
            billOfLadingStore = billOfLadingStream.getKafkaStream()
                .store(
                    StoreQueryParameters.fromNameAndType(
                        BillOfLadingStream.BILLS_OF_LADING_STORE,
                        QueryableStoreTypes.keyValueStore()
                    )
                );
        }
        return billOfLadingStore;
    }
}
