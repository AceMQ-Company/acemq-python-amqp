# Licence and warranty

AceMQ for Python is [Apache License
2.0](https://www.apache.org/licenses/LICENSE-2.0). You may use it in production,
commercially, without asking and without paying.

## No warranty

The licence disclaims warranties and limits liability — sections 7 and 8. In
plain terms: this is provided as it is, and if it loses your messages that is
your risk to have taken.

That is not a formality to skim. It is a young library: pre-1.0, with an API
still free to change, and its own documentation says which parts have been
proven against a real broker and which have not. Read [testing](testing.md) for
what is checked without one and what needs one.

If you need somebody accountable for it working, that is what [Enterprise
support](https://acemq.com) is for. The library is complete and free without it,
and is not crippled to sell it.

## What you must do

Keep the licence and the copyright notice with any copy or derivative, and state
what you changed. That is the whole obligation.

## Dependencies

`pip install acemq-amqp` installs **nothing else**. The contract layer — the
envelope, the codecs, the retry arithmetic, the naming, the topology — is the
standard library and nothing more, UUIDs included.

| | | |
|---|---|---|
| `aio-pika` | Apache-2.0 | The `[rabbitmq]` extra. Used only by `acemq_amqp.rabbitmq`, which `connect` imports at the moment it is called |
| `prometheus-client` | Apache-2.0 | The `[prometheus]` extra. Used only by `acemq_amqp.prometheus` |

A program that only reads an AceMQ envelope off a message somebody else
delivered installs neither. `acemq_amqp.telemetry.prometheus_text` renders a
scrape body using only the standard library, for a service that wants a metrics
endpoint without the second extra.

The development extra — `pip install -e ".[dev]"` — adds `pytest`,
`pytest-asyncio`, `ruff`, `mypy` and `pdoc`. None of it reaches a published
wheel.

## Trademarks

RabbitMQ is a trademark of Broadcom Inc. and/or its subsidiaries. Python is a
trademark of the Python Software Foundation. AceMQ is an independent project,
affiliated with and endorsed by neither.
