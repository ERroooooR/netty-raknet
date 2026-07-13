# netty-raknet
[![Build Status](https://ci.codemc.org/job/yesdog/job/netty-raknet/badge/icon)](https://ci.codemc.org/job/yesdog/job/netty-raknet/)
[![Discord Chat](https://img.shields.io/discord/574240965351571477.svg)](https://discord.gg/MhhWfSW)
[![Known Vulnerabilities](https://snyk.io/test/github/yesdog/netty-raknet/badge.svg)](https://snyk.io/test/github/yesdog/netty-raknet)
[![SonarCloud Bugs](https://sonarcloud.io/api/project_badges/measure?project=yesdog_netty-raknet&metric=bugs)](https://sonarcloud.io/component_measures/metric/reliability_rating/list?id=yesdog_netty-raknet)
[![SonarCloud Coverage](https://sonarcloud.io/api/project_badges/measure?project=yesdog_netty-raknet&metric=coverage)](https://sonarcloud.io/component_measures/metric/coverage/list?id=yesdog_netty-raknet)

High performance RakNet implementation
targeting unreliable and rate-limited client connections. It provides strict netty 
style server and client channels. 

This implementation uses [Netty](https://github.com/netty/netty) 
channels to provide a fast and effective [RakNet](http://www.raknet.net) server, 
offering the full feature set of the transport protocol, while providing
room for extension with any plugins or custom behavior. 

## Features
* Recylable objects:
  * Heavily used objects are recycled.
  * Reduces GC pressure.
  * Instrumented with Netty leak detection.
* Strict Netty patterns:
  * Uses Bootstrap and ServerBootstrap pattern.
  * Signals backpressure using Channel writability. 
  * Uses Netty ChannelOptions for channel config.
  * Follows the normal *bind* and *connect* patterns.
  * Accurate promise responses for *write*, *connect* and others. 
* 0-copy buffer interactions:
  * Retained buffer references throughout.
  * Composite buffers used for encapsulation and defragmentation. 
* Easy-to-use data streaming interface:
  * Configurable packet ID used for raw ByteBuf writing and reading.
  * Extensible to allow for multiple packet ID and channel configurations.
  * True to Netty form, the pipeline can be modified and augmented as needed.
* Advanced flow control
  * Back pressure signals useful for buffer limiting when client is overloaded. 
  * Pending frame-set limits reduce unnecessary resends during high transfer rates.
  * Resend priority based on frame sequence so you get older packets faster.
* Automated flush driver
  * Recommended to write to pipeline with no flush. 
  * Flush cycles condense outbound data for best use of MTU.

## Adaptive transport

Protocol version 11 is the default and remains compatible with maintained Mojang RakNet
implementations. Versions 9 and 10 are still accepted. This fork adds protocol version 12 for
explicitly negotiated transport extensions; v9-v11 never emit extension packets.

Adaptive pacing, rolling-window loss classification, idle-boundary MTU fallback/recovery and
PPS-aware batching are enabled by default. Configure them through Netty channel options:

```java
bootstrap.option(RakNet.ADAPTIVE_TRANSPORT, true);
bootstrap.option(RakNet.ADAPTIVE_DSCP, false); // Shared UDP socket: disabled by default.
bootstrap.option(RakNet.PROTOCOL_VERSION, 12); // Preferred; the client falls back to v11 on rejection.
bootstrap.option(RakNet.ADAPTIVE_MIN_PPS, 50);
bootstrap.option(RakNet.ADAPTIVE_MAX_PPS, 2000);
bootstrap.option(RakNet.SMALL_WRITE_COALESCE_MICROS, 250); // 0 disables the wait window.
bootstrap.option(RakNet.PLPMTUD_MAX_MTU, 1500); // May exceed the handshake MTU.
```

Version 12 negotiates bounded Reed-Solomon FEC. It protects adaptive groups of 8-12 FrameSets with
one or two GF(256) parity shards, recovers up to two losses, and feeds receiver recovery outcomes
back into the sender's redundancy budget. Peers without the Reed-Solomon feature bit retain the
single-parity XOR format. FEC remains off during burst loss and queue congestion.

The DPLPMTUD implementation follows the RFC 8899 BASE, SEARCHING, SEARCH_COMPLETE, ERROR and
DISABLED phases, requires positive token acknowledgement, retries a candidate three times, validates
Packet Too Big bounds, periodically reopens a completed search, and can probe above the handshake
MTU up to `PLPMTUD_MAX_MTU`. Java/native transports can publish validated ICMP PTB and ECN-CE
signals with `TransportFeedbackEvent`; local EMSGSIZE/message-too-long write failures are converted
automatically. MTU changes wait for queued and in-flight frames to drain.

The model congestion controller keeps a ten-second max-delivery-rate filter, minimum RTT, explicit
bytes-in-flight and congestion window, ACK aggregation allowance, STARTUP/DRAIN/PROBE_BW/PROBE_RTT
phases, pacing gains and ECN-CE response. This replaces packet-rate-only growth while retaining the
configured PPS bounds as operational safety limits.

Adaptive DSCP operates on the single shared server socket. It requires at least 16 connection votes,
a 2:1 majority and a 30-second cooldown before switching between AF41 and CS0. Per-player DSCP is not
possible with the shared-socket server architecture.

`RakNet.MetricsLogger` exposes pacing and delivery rates, rolling ACK/loss samples, loss type,
active MTU and DPLPMTUD state, FEC shards/recovery budget, congestion mode/cwnd/in-flight/bandwidth,
ACK aggregation, ECN ratio, shared-socket DSCP changes, coalesced small writes and pacing delay.
Implementations may leave these default methods unused; the transport does not perform blocking
metric export on an event loop.
  
# Usage

## Maven
```xml
    <dependencies>
        <dependency>
            <groupId>network.ycc</groupId>
            <artifactId>raknet-server</artifactId>
            <version>0.8-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>network.ycc</groupId>
            <artifactId>raknet-client</artifactId>
            <version>0.8-SNAPSHOT</version>
        </dependency>
    </dependencies>

    <repositories>
        <repository>
            <id>codemc-repo</id>
            <url>https://repo.codemc.org/repository/maven-public</url>
        </repository>
    </repositories>
```

### Example

A good example can be seen in the simple 
[Hello World](https://github.com/yesdog/netty-raknet/blob/master/tests/src/test/java/network/ycc/raknet/HelloWorld.java) 
test case.

# [License](./LICENSE)
