# Lyra

*Fault tolerant RabbitMQ client*

## Introduction

Dealing with failure is a fact of life in distributed systems. Lyra is a [RabbitMQ](http://www.rabbitmq.com/) client that embraces failure, allowing for AMQP resources such as connections, channels and consumers to be automatically recovered when server or network failures occur.

#### Features

* Automatic resource recovery
* Automatic invocation retries
* Event listeners

## Setup

`mvn install`

## Usage

#### Resource Recovery

The key feature of Lyra is its ability to automatically recover resources such as [Connections][Connection], [Channels][Channel] and [Consumers][Consumer] when unexpected failures occur. To create *recoverable* resources, start by creating an Options object and specifying a recovery policy:

```java
Options options = new Options()
	.withRecoveryPolicy(new RetryPolicy()
		.withMaxRetries(100)
		.withInterval(Duration.seconds(1))
		.withMaxDuration(Duration.minutes(5)));
```

With our `options`, let's create a *recoverable* Connection along with some Channels and Consumers:

```java
Connection connection = Connections.create(options);
Channel channel1 = connection.createChannel(1);
Channel channel2 = connection.createChannel(2);
channel1.basicConsume("foo-queue", consumer1);
channel1.basicConsume("foo-queue", consumer2);
channel2.basicConsume("foo-queue", consumer3);
channel2.basicConsume("foo-queue", consumer4);
```

This results in the dependency hierarchy:

<img src="http://jodah.net/lyra/assets/img/rabbit-graph.png"\>

If the Connection is unexpectedly closed, Lyra will attempt to recover it along with its Channels and Consumers. If a Channel is unexpectedly closed, Lyra will attempt to recover it along with its Consumers.

#### Invocation Retries

Lyra also supports invocation retries when a *retryable* failure occurs while creating a Connection or invoking a method against a [Connection] or [Channel]. For example:

```java
Options options = new Options()
	.withRecoveryPolicy(RetryPolicies.retryAlways())
	.withRetryPolicy(new RetryPolicy()
		.withBackoff(Duration.seconds(1), Duration.seconds(30))
		.withMaxDuration(Duration.minutes(10)));
		
Connection connection = Connections.create(options);
Channel channel = connection.createChannel();
channel.basicConsume("foo-queue", myConsumer);
```

Here we've created a new `Connection` and `Channel`, specifying a recovery policy to use in case any of our resources are *unexpectedly* closed as a result of an invocation failure, and a retry policy that dictates how and when the failed method invocation should be retried. If the `Connections.create()`, `connection.createChannel()`, or `channel.basicConsume()` method invocations fail as the result of a *retryable* error, Lyra will attempt to recover any resources that were closed according to the recovery policy and retry the invocation according to the retry policy.

#### Event Listeners

Lyra offers [listeners](http://jodah.net/lyra/javadoc/net/jodah/lyra/event/package-summary.html) for creation and recovery events:

```java
Options options = new Options();
	.withConnectionListeners(myConnectionListener)
	.withChannelListeners(myChannelListener)
	.withConsumerListeners(myConsumerListener);
```

## Additional Notes

#### On Recovery / Retry Policies

[Recovery / Retry policies](http://jodah.net/lyra/javadoc/net/jodah/lyra/retry/RetryPolicy.html) allow you to specify:

* The maximum number of retry attempts to perform
* The maxmimum duration that retries should be performed for
* The standard interval between retry attempts
* The maximum interval between retries to exponentially backoff to

Lyra allows for Recovery / Retry policies to be set globally or for individual resource types, as well as for initial connection attempts.

#### On Retryable Failures

Lyra will only retry failed invocations that are deemed *retryable*. These include connection errors that are not related to failed authentication, and channel or connection errors that might be the result of temporary network failures.

#### On Threading

When a Connection or Channel are closed as the result of an invocation:

* If recovery and retry policies are configured the calling thread will block until the Connection/Channel is recovered and the retry can be performed.
* If a retry policy is not configured the failure is immediately rethrown. Recovery will still take place in a background thread if a recovery policy is configured.

When a Connection or Channel are closed involuntarily (not as the result of an invocation), recovery occurs in a background thread if a recovery policy is configured.

## Docs

JavaDocs are available [here](https://jhalterman.github.com/lyra/javadoc).

## License

Copyright 2013 Jonathan Halterman - Released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).

[Connection]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Connection.html
[Channel]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Channel.html
[Consumer]: http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Consumer.html