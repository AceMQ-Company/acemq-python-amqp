# Copyright 2026 AceMQ.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""AceMQ for Python.

A client for AceMQ messaging over AMQP, speaking the same wire contract as the
`Java <https://github.com/AceMQ-Company/acemq-java-amqp>`_, `Go
<https://github.com/AceMQ-Company/acemq-go-amqp>`_ and `.NET
<https://github.com/AceMQ-Company/acemq-dotnet-amqp>`_ libraries: the same
reserved headers, the same defaults, the same retry semantics. A Python
consumer reads what a Java producer writes, and the fixtures generated from the
Java implementation pin that rather than leaving it to be discovered in
production.

The API shape is Python's, deliberately. The contract is portable; the
ergonomics are native.

Importing this package needs nothing installed. :func:`connect` reaches for
aio-pika at the moment it is called rather than at import, so a program that
only reads an envelope off a message somebody else delivered never has to have
an AMQP client at all. The blocking API lives in :mod:`acemq_amqp.sync` for
programs that are not running an event loop.

Reaching a real broker means reaching it over TLS: an ``amqps://`` URL is
verified against the machine's trust store with no further configuration, and
:class:`Security` in :mod:`acemq_amqp.security` covers a private certificate
authority, a client certificate, and a login supplied separately from the URL so
that a password never has to be written into a connection string.
"""

from __future__ import annotations

from . import headers
from .ack import Ack, Action, FatalError, accept, reject
from .ack import retry as _retry
from .codec import (
    BYTES_CONTENT_TYPE,
    JSON_CONTENT_TYPE,
    TEXT_CONTENT_TYPE,
    BytesCodec,
    Codec,
    CompositeCodec,
    JsonCodec,
    TextCodec,
    codec_by_name,
    codec_names,
    register_codec,
)
from .connection import (
    AsyncHandler,
    BrokerHealth,
    Connection,
    Consumer,
    Handler,
    Message,
    Publisher,
    connect,
)
from .envelope import Envelope
from .errors import AceMQError, PublishError, SecurityError
from .interceptors import (
    ConsumeContext,
    ConsumeInterceptor,
    ConsumeNext,
    PublishContext,
    PublishInterceptor,
    PublishNext,
    consume_chain,
    publish_chain,
)
from .naming import dead_letter_queue, parked_queue, retry_queue
from .retry import (
    DEFAULT_BROKER_WAIT_THRESHOLD,
    RetryPolicy,
    Wait,
    exponential_retry,
    fixed_retry,
    no_retry,
)
from .security import (
    Credentials,
    CredentialsSource,
    Security,
    Verification,
    credentials_from_environment,
    credentials_from_file,
    without_verifying_the_broker,
)
from .telemetry import (
    METRIC_ACCEPTED,
    METRIC_CONSUMED,
    METRIC_DEAD_LETTERED,
    METRIC_HANDLER_DURATION,
    METRIC_IN_FLIGHT,
    METRIC_PARKED,
    METRIC_PUBLISH_FAILED,
    METRIC_PUBLISHED,
    METRIC_REJECTED,
    METRIC_RETRIED,
    METRIC_RUNG_MISSING,
    METRIC_SET_ASIDE_FAILED,
    DurationSummary,
    HealthCheck,
    HealthReport,
    HealthStatus,
    Metrics,
    NullObserver,
    Observer,
    aggregate_health,
    prometheus_text,
)
from .topology import (
    DEAD_LETTER_EXCHANGE,
    QUEUE_TYPE_ARG,
    QUORUM_QUEUE_TYPE,
    RETRY_EXCHANGE,
    Topology,
    declare_where_failures_go,
    rung_args,
)
from .transport import (
    ConsumeSpec,
    Delivery,
    ExchangeSpec,
    Outbound,
    PublishResult,
    QueueSpec,
    Transport,
)

#: Returns a message to be tried again. See :func:`acemq_amqp.ack.retry`.
#:
#: Bound here rather than imported by name, because importing the retry module a
#: few lines above binds ``acemq_amqp.retry`` to the module and the last write
#: wins. A module is not what ``from acemq_amqp import retry`` should hand
#: somebody when the other three libraries hand them the acknowledgement, and
#: ``from acemq_amqp.retry import RetryPolicy`` still reaches the module.
retry = _retry

__version__ = "0.2.0"

__all__ = [
    "BYTES_CONTENT_TYPE",
    "DEAD_LETTER_EXCHANGE",
    "DEFAULT_BROKER_WAIT_THRESHOLD",
    "JSON_CONTENT_TYPE",
    "METRIC_ACCEPTED",
    "METRIC_CONSUMED",
    "METRIC_DEAD_LETTERED",
    "METRIC_HANDLER_DURATION",
    "METRIC_IN_FLIGHT",
    "METRIC_PARKED",
    "METRIC_PUBLISHED",
    "METRIC_PUBLISH_FAILED",
    "METRIC_REJECTED",
    "METRIC_RETRIED",
    "METRIC_RUNG_MISSING",
    "METRIC_SET_ASIDE_FAILED",
    "QUEUE_TYPE_ARG",
    "QUORUM_QUEUE_TYPE",
    "RETRY_EXCHANGE",
    "TEXT_CONTENT_TYPE",
    "AceMQError",
    "Ack",
    "Action",
    "AsyncHandler",
    "BrokerHealth",
    "BytesCodec",
    "Codec",
    "CompositeCodec",
    "Connection",
    "ConsumeContext",
    "ConsumeInterceptor",
    "ConsumeNext",
    "ConsumeSpec",
    "Consumer",
    "Credentials",
    "CredentialsSource",
    "Delivery",
    "DurationSummary",
    "Envelope",
    "ExchangeSpec",
    "FatalError",
    "Handler",
    "HealthCheck",
    "HealthReport",
    "HealthStatus",
    "JsonCodec",
    "Message",
    "Metrics",
    "NullObserver",
    "Observer",
    "Outbound",
    "PublishContext",
    "PublishError",
    "PublishInterceptor",
    "PublishNext",
    "PublishResult",
    "Publisher",
    "QueueSpec",
    "RetryPolicy",
    "Security",
    "SecurityError",
    "TextCodec",
    "Topology",
    "Transport",
    "Verification",
    "Wait",
    "__version__",
    "accept",
    "aggregate_health",
    "codec_by_name",
    "codec_names",
    "connect",
    "consume_chain",
    "credentials_from_environment",
    "credentials_from_file",
    "dead_letter_queue",
    "declare_where_failures_go",
    "exponential_retry",
    "fixed_retry",
    "headers",
    "no_retry",
    "parked_queue",
    "prometheus_text",
    "publish_chain",
    "register_codec",
    "reject",
    "retry",
    "retry_queue",
    "rung_args",
    "without_verifying_the_broker",
]
