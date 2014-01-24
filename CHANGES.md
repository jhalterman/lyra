# 0.4.0

### New Features

* Added support for exchange, exchange binding, queue, and queue binding recovery. Exchanges, queues and bindings are tracked per connection.

### Bug Fixes

* Fixed issue #25 - Proxy is uninitialized when ConnectionListener.onCreate is called.
* Fixed issue #23 - Recycle conneciton pools.
* Fixed issue #24 - Adds "Recovered" log line for successful channel recovery.
* Fixed issue #22 - Ability to parse infinite duration from String.

# 0.3.2

### API changes:

* RecoveryPolicy was broken out into a separate class.
* RetryPolicy was moved to the `net.jodah.lyra.config` package.
* ConnectionListener.onChannelRecovery was added.
* ChannelListener.onConsumerRecovery was added.

### Behavioral changes:

* When a channel is closed and recovered, attempts to ack/nack/reject messages that were delivered before the channel was recovered are simply ignored since their delivery tags will be invalid for the newly recovered channel. Instead these messages will be re-delivered with new delivery tags.
* ConnectionListener.onRecovery is now called after the connection is recovered, but before the channels are recovered. ConnectionListener.onChannelRecovery is called after the connection and channels are recovered.
* ChannelListener.onRecovery is now called after the channel is recovered, but before the consumers are recovered. ChannelListener.onConsumerRecovery is called after the channel and consumers are recovered.

### Bug Fixes

* Fixed issue #18 Unable to invoke withConsumerListeners() on ConfigurableChannel.
* Fixed issue #13 Fix ConnectionOptions.withAddresses using Address list.