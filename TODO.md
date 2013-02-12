### 0.0.1
  * AsyncProducer
    - Implement a bounded queue, sending thread, etc
  * Ensure that protocol errors are being handled correctly and not bubbling up
  * More integration tests, replication, leader changes, etc. Investigate intreseting cases kafka tests
  * Cleanup: extract protocol struct delegation to a module.

### 0.0.2

  * New Consumer/Consumer Enhancements
    - Automatically partition work among consumers (zookeeper, redis, pluggable?)
    - Offset API (even tho it won't be in a stable ver for a while)
    - Handle case where the offset we're trying to read from no longer exists

  * Snappy Compression
    - snappy: c-ext, would like to avoid
    - snappy_ffi: ffi interface, but needs to be updated (pre c-api) 
        and has no specs, docs. Also linked to a c-ext version, two gems, etc..
    - new snappy ffi library with specs, docs, etc. Shave that Yak!

  * Benchmark/Profiling. KGIO?
