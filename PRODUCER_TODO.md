- Get metadata crap to work correctly.

- Speeding up specs
- ACKS!
- Keep going down the current path?
- Should we use the ERRROR array for the selector? Probabaly send all connections to it?
- Selector: add broker id to Stream instead of inverted index
- Thread safety?!
- Add an option to automatically run callbacks in their own thread pool?

# Cleanup

- Investigate crazy metadata update stuff

# New tests

* Close actually spins down the other thread
* Augment the compression tests with something that actually verifies with was compressed!
