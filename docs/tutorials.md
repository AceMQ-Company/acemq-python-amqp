# Tutorials

Step by step, in order, each one ending with something that runs.

The [guide](index.md) explains how a thing works and why it is that way. These
are the other shape: start with nothing, finish with a working service, and
understand what you typed by the end rather than before the beginning.

| | | |
|---|---|---|
| 1 | [Your first message](tutorial-first-message.md) | Connect, declare, publish, consume | 10 min |
| 2 | [Surviving failure](tutorial-surviving-failure.md) | Retries that do not block, dead letters, and replaying them | 20 min |
| 3 | [Never processing twice](tutorial-exactly-once.md) | Idempotency, the outbox, and why "exactly once" is a lie | 25 min |
| 4 | [Seeing what happens](tutorial-observability.md) | Metrics, health and traces, and reading them when something is wrong | 20 min |

Each builds on the one before it, and the same four subjects are taught in the
same order in all five AceMQ libraries — so tutorial 3 teaches the same thing
whichever language you came from, in that language's own shape rather than in a
translation of Java's.

## Before you start

```bash
pip install "acemq-amqp[rabbitmq]"
```

Python 3.10 or newer.

**All four need a broker**, and that is a difference worth stating up front: the
Java and .NET versions of tutorial 1 run against an in-process broker behind a
`memory://` URL, and there is no such thing here. `Transport` is a six-method
`Protocol`, so the fake a *test* needs is a small class rather than a shipped
subsystem — the right trade for tests, and one that costs you a `docker run`
here.

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

The management UI is at <http://localhost:15672>, guest/guest. Tutorial 3 also
uses `sqlite3`, which is in the standard library.

What you can do without a broker is test the part most worth testing. A handler
is a function from a `Message` to an `Ack` and both are ordinary values, so most
of a service's logic is decidable in milliseconds and none of it involves Docker
— see [testing without a broker](testing.md).

## Everything here is asyncio

The library is async because a broker client is: every publish is a round trip
and every delivery arrives on its own, and hiding that behind blocking calls
would make one slow queue stall a whole process.

If your program is not running a loop,
[`acemq_amqp.sync`](getting-started.md#not-running-an-event-loop) is the same
connection behind a thread of its own, with handlers on worker threads — a
facade, not a second implementation, so there is exactly one set of retry
arithmetic in this library. Everything in these four tutorials has a blocking
equivalent except [request and reply](request-reply.md#from-a-program-with-no-event-loop).

## If you would rather read code

The [examples repository](https://github.com/AceMQ-Company/acemq-python-amqp-examples)
has runnable programs, each verified by CI on every commit. Tutorials teach;
examples demonstrate. Start here, go there when you want to see a whole system
rather than one idea.
