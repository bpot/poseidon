### 0.0.1
  * Ensure that protocol errors are being handled correctly and not bubbling up
  * More integration tests, replication, leader changes, etc. Investigate interesting cases in kafka's tests
  * End-to-end integration specs
    - In specs that test broker failure, verify that messages were actually sent/not sent with a consumer.
 
  * AsyncProducer
    - Implement a bounded queue, sending thread, etc
  * Cleanup: extract protocol struct delegation to a module.
  * When failing to send messages in sync producer, return messages that failed to send?

### 0.0.2

  * New Consumer/Consumer Enhancements
    - Automatically partition work among consumers (zookeeper, redis, pluggable?)
    - Handle case where the offset we're trying to read from no longer exists

  * Snappy Compression
    - snappy: c-ext, would like to avoid
    - snappy_ffi: ffi interface, but needs to be updated (pre c-api) 
        and has no specs, docs. Also linked to a c-ext version, two gems, etc..
    - new snappy ffi library with specs, docs, etc. Shave that Yak!

  * Benchmark/Profiling. KGIO?

### 0.0.3 -- Targets Kafka 0.8.1
  - Offset API
