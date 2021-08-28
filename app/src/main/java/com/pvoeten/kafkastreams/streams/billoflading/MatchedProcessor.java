package com.pvoeten.kafkastreams.streams.billoflading;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class MatchedProcessor implements Processor<String, BillOfLadingProjection> {

  private KeyValueStore<String, BillOfLadingProjection> billsOfLadingBuffer;
  private KeyValueStore<String, BillOfLadingProjection> billOfLadingStore;

  @Override
  public void init(ProcessorContext context) {
    billsOfLadingBuffer = context.getStateStore(BillOfLadingStream.BILLS_OF_LADING_BUFFER);
    billOfLadingStore = context.getStateStore(BillOfLadingStream.BILLS_OF_LADING_STORE);
  }

  @Override
  public void process(String key, BillOfLadingProjection value) {
    billsOfLadingBuffer.delete(key);
    billOfLadingStore.put(key, value);
  }

  @Override
  public void close() {
    // do nothing
  }
}
