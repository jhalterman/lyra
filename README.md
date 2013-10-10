# Lyra

*Fault tolerant RabbitMQ client*

## Introduction

Dealing with failure is a fact of life in distributed systems. Lyra is a [RabbitMQ](http://www.rabbitmq.com/) client that embraces failure, allowing for AMQP resources such as connections, channels and consumers to be automatically recovered when network failures occur.

## Features

* Automatic resource recovery
* Automatic invocation retries
* Connection and Channel pooling

## Setup

Until the initial release to Maven Central, you can install Lyra via Maven:

`mvn install`

## User Guide

### Recoverable Resources

Lyra extends the [Java AMQP client](http://www.rabbitmq.com/java-client.html) to provide *recoverable* resources such as [Connections](http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Connection.html), [Channels](http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Channel.html) and [Consumers](http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/com/rabbitmq/client/Consumer.html). Let's create some recoverable resources:

```
LyraOptions options = LyraOptions.forHost("localhost")
	.withRecoveryPolicy(new RetryPolicy()
		.withMaxRetries(100)
		.withInterval(Duration.seconds(1))
		.withMaxDuration(Duration.minutes(5)));

Connection connection = Connections.create(options);
Channel channel = connection.createChannel();
channel.basicConsume("foo-queue", myConsumer);
```

We've created a new Connection through the `Connections` class, specifying a recovery policy to use in case any of our resources are *unexpectedly* closed and need to be recovered. If the consumer is cancelled, the channel is closed, or the connection is closed as the result of a *recoverable* server error or a network failure, Lyra will attempt to recover the closed resources automatically, according to the recovery policy.

### Retryable Invocation

Lyra also support *retryable* invocation against resources:

```
LyraOptions options = LyraOptions.forHost("localhost"))
	.withRetryPolicy(new RetryPolicy()
		.withBackoff(Duration.seconds(1), Duration.seconds(30))
		.withMaxDuration(Duration.minutes(10)));
		
Connection connection = Connections.create(options);
```

With a retry policy specified, any invocation against a resource, including initial connection attempts, will be automatically retried according to the policy, should it fail for some *recoverable* reason. If the invocation failure results in the resource being closed, as is often the case with channels, then the resource will automatically be recovered before the invocation is retried.

### More on Retry / Recovery Policies

Retry / Recovery policies allow you to specify the maximum number of retry attempts to perform, the maxmimum duration that retries should be performed for, the standard interval between retry attempts, and the maximum interval between retries to exponentially backoff to.

Lyra allows for Retry / Recovery policies to be set globally or for individually for specific resource types, including for initial connect attempt.

### More on Recoverable Failures

Lyra will only recover or retry after a failure that is deemed *recoverable*. These include connection attempts that are not authentication failures, and channel or connection errors that might be the result of temporary problems.

### Connection Pooling



### Channel Pooling



## License

Copyright 2013 Jonathan Halterman - Released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).