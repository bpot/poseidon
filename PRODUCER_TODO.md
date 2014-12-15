- Compression
- ACKS!
- Document and support all teh options!
- Should we use the ERRROR array for the selector? Probabaly send all connections to it?
- Selector: add broker id to Stream instead of inverted index
- Thread safety?!
- Add an option to automatically run callbacks in their own thread pool?
- More retriable exceptions


# Cleanup
- Consistently use `node` and `node_id`
- Clean up ClientRequest/NetworkSend/etc crap 
- Organize classes?

# Ruby Improvements

- Investigate crazy timeout stuff, make it more declarative.
- Investigate if a state machine approach would make sense for any of this?
- Use hashes instead of structs where it makes sense.
- Fix purgatory issue, possibly also submit patch for issue for Java version.

# New tests

* Close actually spins down the other thread
* Augment the compression tests with something that actually verifies with was compressed!
* Bringing down a broker with and without retries turned on, one should error, the other should succeed
