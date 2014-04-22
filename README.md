# Lyra

*High availability RabbitMQ client*

## Introduction

Dealing with failure is a fact of life in distributed systems. Lyra is a [RabbitMQ](http://www.rabbitmq.com/) client that embraces failure, helping you achieve high availability in your services by automatically recovering AMQP resources when [unexpected failures][failure-scenarios] occur. Lyra also supports automatic invocation retries, recovery related eventing, and exposes a simple, lightweight API built around the [Java AMQP client](java-client) library.

## Motivation

Lyra was created with the simple goal of recovering client created AMQP resources from **any** RabbitMQ failure that could reasonably occur. While nothing like this existed when Lyra was created, the [Java AMQP client][java-client] now supports [automatic recovery](http://www.rabbitmq.com/api-guide.html#recovery) as well. But while the AMQP client can recover from connection failures, Lyra provides the ability to recovery from [any type of failure][failure-scenarios], including Channel and Consumer closures, while providing flexible [recovery policies][recovery-policy], [configuration][config], and [eventing] to help you get you services back online.

## Setup

Add the Lyra dependency:

```xml
<dependency>
  <groupId>net.jodah</groupId>
  <artifactId>lyra</artifactId>
  <version>0.4.0</version>
</dependency>
```

Also add the latest [amqp-client] dependency:

```xml
<dependency>
  <groupId>com.rabbitmq</groupId>
  <artifactId>amqp-client</artifactId>
  <version>3.3.0</version>
</dependency>
```

## Usage

#### Automatic Resource Recovery

The key feature of Lyra is its ability to *automatically* recover resources such as [connections][Connection], [channels][Channel], [consumers][Consumer], exchanges, queues and bindings when [unexpected failures][failure-scenarios] occur. Lyra provides a flexible policy to define how recovery should be performed.

To start, create a `Config` object, specifying a recovery policy:

```java
Config config = new Config()
	.withRecoveryPolicy(new RecoveryPolicy()
		.withMaxAttempts(20)
		.withInterval(Duration.seconds(1))
		.withMaxDuration(Duration.minutes(5)));
```

With our `config`, let's create some *recoverable* resources:

```java
ConnectionOptions options = new ConnectionOptions().withHost("localhost");
Connection connection = Connections.create(options, config);
Channel channel1 = connection.createChannel(1);
Channel channel2 = connection.createChannel(2);
channel1.basicConsume("foo-queue", consumer1);
channel1.basicConsume("foo-queue", consumer2);
channel2.basicConsume("bar-queue", consumer3);
channel2.basicConsume("bar-queue", consumer4);
```

This results in the resource topology:

<img src="https://raw.github.com/jhalterman/lyra/gh-pages/assets/img/rabbit-graph.png"\>

If a connection or channel is unexpectedly closed, Lyra will attempt to recover it along with its dependents according to the recovery policy. In addition, any non-durable or auto-deleting exchanges and queues, along with their bindings, will be recovered as needed.

#### Automatic Invocation Retries

Lyra also supports invocation retries when a *retryable* failure occurs while creating a Connection or invoking a method against a [Connection] or [Channel]. Similar to recovery, retries are also performed according to a policy:

```java
Config config = new Config()
	.withRecoveryPolicy(RecoveryPolicies.recoverAlways())
	.withRetryPolicy(new RetryPolicy()
		.withBackoff(Duration.seconds(1), Duration.seconds(30))
		.withMaxDuration(Duration.minutes(10)));

ConnectionOptions options = new ConnectionOptions().withHost("localhost");
Connection connection = Connections.create(options, config);
Channel channel = connection.createChannel();
channel.basicConsume("foo-queue", myConsumer);
```

Here we've created a new `Connection` and `Channel`, specifying a recovery policy to use in case any of our resources are *unexpectedly* closed as a result of an invocation failure, and a retry policy that dictates how and when the failed method invocation should be retried. If any method invocation such as `Connections.create()`, `connection.createChannel()`, or `channel.basicConsume()` fails as the result of a *retryable* error, Lyra will attempt to recover any resources that were closed according to the recovery policy and retry the invocation according to the retry policy.

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


## Additional Notes

#### On Recovery and Retry Policies

[Recovery][recovery-policy] and [Retry][retry-policy] policies allow you to specify:

* The maximum number of attempts to perform
* The maxmimum duration that attempts should be performed for
* The interval between attempts
* The maximum interval between attempts to exponentially backoff to

Lyra allows for recovery and retry policies to be set globally, for individual resource types, and for initial connection attempts.

#### On Retryable Failures

Lyra will only retry failed invocations that are deemed *retryable*. These include connection errors that are not related to failed authentication, and channel or connection errors that might be the result of temporary network failures.

#### On Publishing

When a channel is closed and is in the process of being recovered, attempts to publish to that channel will result in `AlreadyClosedException` being thrown. Publishers should either wait and listen for recovery by way of a [ChannelListener][channel-listener], or use a [RetryPolicy][retry-policy] to retry publish attempts once the channel is recovered.

#### On Message Delivery

When a channel is closed and recovered, any messages that were delivered but not acknowledged will be redelivered on the newly recovered channel. Attempts to ack/nack/reject messages that were delivered before the channel was recovered are simply ignored since their delivery tags will be invalid for the newly recovered channel. 

Note, since channel recovery happens transparently, in effect when a channel is recovered and message redelivery occurs **messages may be seen more than once on the recovered channel**.

#### On Threading

When a Connection or Channel are closed unexpectedly recovery occurs in a background thread. If a retry policy is configured then any invocation attempts will block the calling thread until the Connection/Channel is recovered and the invocation can be retried.

## Additional Resources

* JavaDocs are available [here](https://jhalterman.github.com/lyra/javadoc).
* The various failure scenarios handled by Lyra are described [here][failure-scenarios].
* See the [Lyra cookbook][cookbook] for handling specific RabbitMQ use cases.

## Thanks

Thanks to Brett Cameron, Michael Klishin and Matthias Radestock for their valuable ideas and feedback.

## License

Copyright 2013-2014 Jonathan Halterman - Released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).

[java-client]: http://www.rabbitmq.com/java-client.html
[Connection]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Connection.html
[Channel]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Channel.html
[Consumer]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Consumer.html
[amqp-client]: http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22amqp-client%22
[before-consumer-recovery]: http://jodah.net/lyra/javadoc/net/jodah/lyra/event/ConsumerListener.html#onBeforeRecovery(com.rabbitmq.client.Consumer%2C%20com.rabbitmq.client.Channel)
[connect-retry]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html#withConnectRetryPolicy(net.jodah.lyra.retry.RetryPolicy)
[global-recovery]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html#withRecoveryPolicy(net.jodah.lyra.retry.RetryPolicy)
[global-retry]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html#withRetryPolicy(net.jodah.lyra.retry.RetryPolicy)
[config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/Config.html
[channel-listener]: http://jodah.net/lyra/javadoc/net/jodah/lyra/event/ChannelListener.html
[connection-config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/ConnectionConfig.html
[channel-config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/ChannelConfig.html
[consumer-config]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/ConsumerConfig.html
[recovery-policy]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/RecoveryPolicy.html
[retry-policy]: http://jodah.net/lyra/javadoc/net/jodah/lyra/config/RetryPolicy.html
[eventing]: http://jodah.net/lyra/javadoc/net/jodah/lyra/event/package-summary.html
[cookbook]: https://github.com/jhalterman/lyra/wiki/Lyra-Cookbook
[failure-scenarios]: https://github.com/jhalterman/lyra/wiki/Failure-Scenarios