# Native (librdkafka) Consumer & Producer

- they do not support 100% all features of sinek
- they have a slightly different API compared to the connect variants
- they perform a little better than the other clients
- they support SASL (kerberos)
- you can work directly with Buffers

## Setup (required actions to use the clients)

- `npm i -g yarn` # make sure to have yarn available

### Debian/Ubuntu
- `sudo apt install librdkafka-dev libsasl2-dev`
- `rm -rf node_modules`
- `yarn` # node-rdkafka is installed as optional dependency

### MacOS
- `brew upgrade librdkafka`
- `brew upgrade openssl`
- `rm -rf node_modules`
- `yarn` # node-rdkafka is installed as optional dependency

## Using NConsumer & NProducer

- the API is the almost the same as the [Connect Clients](../connect)
- the only difference is that the clients are prefixed with an `N`
- so exchange `const {Consumer, Producer} = require("sinek");` with `const {NConsumer, NProducer} = require("sinek");`

## New/Additional Configuration Parameters

- as usual sinek tries to be as polite as possible and will offer you to use the same
config that you are used to use with the other clients
- however `librdkafka` brings a whole lot of different config settings and parameters
- you can overwrite them (or use interely) with a config sub-object called `noptions`
- e.g. `const config = { noptions: { "metadata.broker.list": "localhost:9092"} };`
- a full list and descriptions of config params can be found [CONFIG HERE](https://github.com/edenhill/librdkafka/blob/0.9.5.x/CONFIGURATION.md)
- producer poll interval can be configured via `const config = { options: { pollIntervalMs: 100 }};`, default is 100ms
- when `noptions` is set, you do not have to set the old config params

## Consumer Modes
- 1 by 1 mode (by passing a callback to .consume()) see `/test/int/NSinek.test.js` -> consumes a single message and commit after callback each round
- asap mode (by passing no callback to .consume()) see `/test/int/NSinekF.test.js` -> consumes messages as fast as possible

## Buffer, String or JSON as message values
- you can call `producer.send()` with a string or with a Buffer instance
- you can only call `producer.bufferXXX()` methods with objects
- you can consume buffer message values with `consumer.consume(_, false, false)`
- you can consume string message values with `consumer.consume(_, true, false)` // this is the default
- you can consume json message values with `consumer.consume(_, true, true)`

## Debug Messages
- if you do not pass a logger via config: `const config = { logger: { debug: console.log, info: console.log, .. }};`
- the native clients will use the debug module to log messages
- use e.g. `DEBUG=sinek:n* npm test`

## BREAKING CHANGES CONSUMER API (compared to connect/Consumer):
- there is an optional options object for the config named: noptions
- pause and resume have been removed
- consumeOnce is not implemented
- backpressure mode is not implemented (given in 1 message only commit mode)
- 1 message only & consume asap modes can be controlled via consumer.consume(syncEvent);
(if syncEvent is present it will consume & commit single messages on callback)
- lastProcessed and lastReceived are now set to null as default value
- closing will reset stats
- no internal async-queue is used to manage messages

## BREAKING CHANGES PRODUCER API (compared to connect/Producer):
- send does not support arrays of messages
- compressionType does not work anymore
- no topics are passed in the constructor
- send can now exactly write to a partition or with a specific key
- there is an optional options object for the config named: noptions
- there is an optional topic options object for the config named: tconf
- sending now rejects if paused
- you can now define a strict partition for send bufferFormat types
- _lastProcessed is now null if no message has been send
- closing will reset stats