# Lyra

*High availability RabbitMQ client*

## Introduction

Dealing with failure is a fact of life in distributed systems. Lyra is a [RabbitMQ](http://www.rabbitmq.com/) client that embraces failure, helping you achieve high availability in your services by automatically recovering AMQP resources such as connections, channels and consumers when server or network failures occur. Lyra also supports automatic invocation retries, and exposes a simple, lightweight API built around the [Java AMQP client](http://www.rabbitmq.com/java-client.html) library.

## Setup

Add the Lyra dependency:

```xml
<dependency>
  <groupId>net.jodah</groupId>
  <artifactId>lyra</artifactId>
  <version>0.3.2</version>
</dependency>
```

Also add the latest [amqp-client] dependency:

```xml
<dependency>
  <groupId>com.rabbitmq</groupId>
  <artifactId>amqp-client</artifactId>
  <version>3.2.1</version>
</dependency>
```

## Usage

#### Resource Recovery

The key feature of Lyra is its ability to automatically recover resources such as [Connections][Connection], [Channels][Channel] and [Consumers][Consumer] when unexpected failures occur. To start, create a `Config` object, specifying a recovery policy:

```java
Config config = new Config()
	.withRecoveryPolicy(new RecoveryPolicy()
		.withMaxAttempts(20)
		.withInterval(Duration.seconds(1))
		.withMaxDuration(Duration.minutes(5)));
```

With our `config`, let's create a *recoverable* Connection along with some Channels and Consumers:

```java
ConnectionOptions options = new ConnectionOptions()
	.withHost("localhost");
Connection connection = Connections.create(options, config);
Channel channel1 = connection.createChannel(1);
Channel channel2 = connection.createChannel(2);
channel1.basicConsume("foo-queue", consumer1);
channel1.basicConsume("bar-queue", consumer2);
channel2.basicConsume("foo-queue", consumer3);
channel2.basicConsume("bar-queue", consumer4);
```

This results in the resource hierarchy:

<img src="https://raw.github.com/jhalterman/lyra/gh-pages/assets/img/rabbit-graph.png"\>

If one of these resources is unexpectedly closed, it will be recovered according to the recovery policy. If the Connection is unexpectedly closed, Lyra will attempt to recover it along with its Channels and Consumers. If a Channel is unexpectedly closed, Lyra will attempt to recover it along with its Consumers.

#### Invocation Retries

Lyra also supports invocation retries when a *retryable* failure occurs while creating a Connection or invoking a method against a [Connection] or [Channel]. For example:

```java
ConnectionOptions options = new ConnectionOptions()
	.withHost("localhost");
Config config = new Config()
	.withRecoveryPolicy(RecoveryPolicies.recoverAlways())
	.withRetryPolicy(new RetryPolicy()
		.withBackoff(Duration.seconds(1), Duration.seconds(30))
		.withMaxDuration(Duration.minutes(10)));
		
Connection connection = Connections.create(options, config);
Channel channel = connection.createChannel();
channel.basicConsume("foo-queue", myConsumer);
```

Here we've created a new `Connection` and `Channel`, specifying a recovery policy to use in case any of our resources are *unexpectedly* closed as a result of an invocation failure, and a retry policy that dictates how and when the failed method invocation should be retried. If the `Connections.create()`, `connection.createChannel()`, or `channel.basicConsume()` method invocations fail as the result of a *retryable* error, Lyra will attempt to recover any resources that were closed according to the recovery policy and retry the invocation according to the retry policy.

#### Resource Configuration

Lyra allows for resource configuration to be applied at different levels. For example, [global recovery][global-recovery] and [global retry][global-retry] policies can be configured for all resources. These policies can be overriden with specific policies for [connection attempts][connect-retry], [connections][connection-config] and [channels][channel-config]. Lyra also allows for individual connections and channels to be re-configured after creation:

```java
ConfigurableConnection configurableConnection = Config.of(connection);
ConfigurableChannel configurableChannel = Config.of(channel);
```

#### Event Listeners

Lyra offers [listeners](http://jodah.net/lyra/javadoc/net/jodah/lyra/event/package-summary.html) for creation and recovery events:

```java
Config config = new Config();
	.withConnectionListeners(myConnectionListener)
	.withChannelListeners(myChannelListener)
	.withConsumerListeners(myConsumerListener);
```

Event listeners can be useful for setting up additional resources during recovery, such as auto-deleted exchanges and queues.

## Cookbook

See the [Lyra cookbook](https://github.com/jhalterman/lyra/wiki/Lyra-Cookbook) for handling various Rabbit use cases.


## Additional Notes

#### On Recovery and Retry Policies

[Recovery][recovery-policy] and [retry][retry-policy] policies allow you to specify:

* The maximum number of attempts to perform
* The maxmimum duration that attempts should be performed for
* The interval between attempts
* The maximum interval between attempts to exponentially backoff to

Lyra allows for recovery and retry policies to be set globally, for individual resource types, and for initial connection attempts.

#### On Retryable Failures

Lyra will only retry failed invocations that are deemed *retryable*. These include connection errors that are not related to failed authentication, and channel or connection errors that might be the result of temporary network failures.

#### On Threading

When a Connection or Channel are closed unexpectedly recovery occurs in a background thread. If the resource was closed as the result of an invocation and a retry policy is configured, the calling thread will block until the Connection/Channel is recovered and the retry can be performed.

#### On Message Delivery

When a channel is closed and recovered, any messages that were delivered but not acknowledged will be redelivered on the newly recovered channel. Attempts to ack/nack/reject messages that were delivered before the channel was recovered are simply ignored since their delivery tags will be invalid for the newly recovered channel. 

Note, since channel recovery happens transparently, in effect when a channel is recovered and message redelivery occurs **messages may be seen more than once on the recovered channel**.

## Docs

JavaDocs are available [here](https://jhalterman.github.com/lyra/javadoc).

## License

Copyright 2013 Jonathan Halterman - Released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).

[Connection]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Connection.html
[Channel]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Channel.html
[Consumer]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Consumer.html
[amqp-client]: http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22amqp-client%22
[before-consumer-recovery]: http://jodah.net/lyra/javadoc/net/jodah/lyra/event/ConsumerListener.html#onBeforeRecovery(com.rabbitmq.client.Consumer%2C%20com.rabbitmq.client.Channel)
[connect-retry]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html#withConnectRetryPolicy(net.jodah.lyra.retry.RetryPolicy)
[global-recovery]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html#withRecoveryPolicy(net.jodah.lyra.retry.RetryPolicy)
[global-retry]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html#withRetryPolicy(net.jodah.lyra.retry.RetryPolicy)
[connection-config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/ConnectionConfig.html
[channel-config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/ChannelConfig.html
[consumer-config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/ConsumerConfig.html
[recovery-policy]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/RecoveryPolicy.html
[retry-policy]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/RetryPolicy.html