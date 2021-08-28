package com.pvoeten.kafkastreams.api;

import com.pvoeten.kafkastreams.service.VesselVisitService;
import com.pvoeten.kafkastreams.streams.billoflading.BillOfLadingProjection;
import com.pvoeten.kafkastreams.streams.billoflading.BillOfLadingStream;
import com.pvoeten.kafkastreams.streams.vesselvisit.VesselVisitStream;
import com.pvoeten.kafkastreams.vesselvisit.VesselVisit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.time.Instant;
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

  private final VesselVisitService vesselVisitService;

  public ApiController(VesselVisitStream vesselVisitStream, BillOfLadingStream billOfLadingStream, HostInfo hostInfo, RestTemplate restTemplate, VesselVisitService vesselVisitService) {
    this.vesselVisitStream = vesselVisitStream;
    this.billOfLadingStream = billOfLadingStream;
    this.hostInfo = hostInfo;
    this.restTemplate = restTemplate;
    this.vesselVisitService = vesselVisitService;
  }

  @GetMapping(path = "/metadata")
  public List<String> getMetadata() {
    Collection<StreamsMetadata> streamsMetadata = vesselVisitStream.getKafkaStream().allMetadataForStore(VesselVisitStream.VESSEL_VISITS_STORE);
    return streamsMetadata.stream().map(StreamsMetadata::toString).collect(Collectors.toList());
  }

  @GetMapping(path = "/bills-of-lading/{id}")
  public ResponseEntity<BillOfLadingProjection> getBillOfLadingById(@PathVariable("id") String id) {
    final KafkaStreams kafkaStream = billOfLadingStream.getKafkaStream();
    if (kafkaStream.state() != KafkaStreams.State.RUNNING) {
      log.info("State store is NOT running");
      return ResponseEntity.notFound().build();
    }

    KeyQueryMetadata keyQueryMetadata = kafkaStream.queryMetadataForKey(BillOfLadingStream.BILLS_OF_LADING_STORE, id, Serdes.String().serializer());
    if (keyQueryMetadata == null) {
      log.info("Bill of Lading with id [{}] does not exist", id);
      return ResponseEntity.notFound().build();
    }
    log.info("Bill of Lading with id [{}] exists on partition {}", id, keyQueryMetadata.partition());
    HostInfo activeHost = keyQueryMetadata.activeHost();
    if (hostInfo.equals(activeHost)) {
      log.info("Bill of Lading with id [{}] is here!!", id);
      return ResponseEntity.ok(getBillOfLadingStore().get(id));
    }
    log.info("Bill of Lading with id [{}] might exist on host {}", id, activeHost);

    BillOfLadingProjection billOfLading = getBillOfLading(id, hostInfo);
    if (billOfLading == null) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(billOfLading);
  }

  @GetMapping(path = "/vesselvisits/{id}")
  public ResponseEntity<VesselVisit> getVesselVisitById(@PathVariable("id") String id) {
    final KafkaStreams kafkaStream = vesselVisitStream.getKafkaStream();
    if (kafkaStream.state() == KafkaStreams.State.REBALANCING) {
      log.info("State store is REBALANCING");
      return ResponseEntity.notFound().build();
    }
    KeyQueryMetadata keyQueryMetadata = kafkaStream.queryMetadataForKey(VesselVisitStream.VESSEL_VISITS_STORE, id, Serdes.String().serializer());
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
    ResponseEntity<VesselVisit> response = restTemplate.getForEntity(url, VesselVisit.class);
    if (response.getStatusCode().isError() || !response.hasBody()) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(response.getBody());
  }

  @PutMapping(path = "/vesselvisits")
  public ResponseEntity<Void> createVesselVisit(@RequestBody VesselVisit vesselVisit) {
    final VesselVisit newVesselVisit = vesselVisit.toBuilder().updated(Instant.now()).build();
    try {
      vesselVisitService.publish(newVesselVisit);
      String url = String.format("http://%s:%s/api/vesselvisits/%s", "localhost", "9000", newVesselVisit.getId());
      return ResponseEntity.created(URI.create(url)).build();
    } catch (Exception e) {
      log.error("", e);
    }
    return ResponseEntity.badRequest().build();
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

  public BillOfLadingProjection getBillOfLading(String bltId, HostInfo activeHost) {
    try {

      String url = String.format("http://%s:%s/api/bills-of-lading/%s", activeHost.host(), activeHost.port(), bltId);
      ResponseEntity<BillOfLadingProjection> response = restTemplate.getForEntity(
          url,
          BillOfLadingProjection.class);
      if (response.getStatusCode().isError() || !response.hasBody()) {
        return null;
      }
      return response.getBody();
    } catch (RestClientException e) {
      return null;
    }
  }

}
