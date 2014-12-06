# 0.0.5

* Add support for negative offsets. [GH-24]
* Fix serious bug where we would send messages to the wrong partition. [GH-36] (Thanks @sclasen and @jorgeortiz85 for tracking this down.)
* Better error message when we can't connect to a broker. [GH-42]
* Handle broker rebalances. [GH-43]
* PartitionConsumer: Block for messages by default. [GH-48]
* Add a logger to help debug issues. [GH-51]
* Add snappy support. [GH-57]
* Allow `:none` value for `:compression_codec` option. [GH-72]
* Allow request buffer to accept mixed encodings. [GH-74]

# 0.0.4

* Don't truncate UTF8 Messages [GH-18]
* Gracefully handle truncated fetch reponses [GH-19]

# 0.0.3

* Better distribute messages across partitions.
* Handle broken connections better.
* Gracefully handle attempts to send an empty set of messages.

# 0.0.2

* Added ability to create a partitioner consumer for a topic+partition using topic metadata.
* Added PartitionConsumer#offset to return offset of the last fetch

# 0.0.1

* Initial release
