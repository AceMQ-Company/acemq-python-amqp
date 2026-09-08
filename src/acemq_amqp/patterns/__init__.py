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

"""The things everybody writes on top of a broker, written once.

Nothing here is new. Every service that consumes a queue eventually grows a way
to ignore a message it has already handled, a way to publish and commit without
losing one, a way to ask a question and wait for the answer. They get written
again in each service, slightly differently, and the differences are where the
bugs live.

These are the same patterns as the Go library's ``patterns`` package and mean
the same things, because a routing slip written by a Go service has to be
readable by a Python one. What they are *not* is the same API: Go takes a
``ctx`` and returns an ``error``, and Python takes keyword arguments and raises.
The behaviour is portable; the shape is native.

Everything in here is built out of the public library — handlers, envelopes,
publishers — so nothing is possible with a pattern that would not be possible
without it. That is deliberate: a pattern you cannot walk away from is a trap.
"""

from __future__ import annotations

from .claimcheck import (
    DEFAULT_THRESHOLD,
    ClaimCheckCodec,
    ClaimCheckStore,
    FilesystemClaimCheckStore,
    InMemoryClaimCheckStore,
    claim_key_of,
    is_claim_check,
)
from .consumergroup import ConsumerGroup
from .idempotency import (
    DEFAULT_MEMORY,
    IdempotencyStore,
    InMemoryIdempotencyStore,
    idempotent,
)
from .ordered import (
    PartitionKey,
    by_correlation,
    by_header,
    ordered,
    partition,
    partitioned_routing_key,
)
from .outbox import (
    DEFAULT_BATCH,
    DEFAULT_INTERVAL,
    InMemoryOutboxStore,
    OutboxRecord,
    OutboxRelay,
    OutboxStore,
    record,
)
from .pipeline import (
    NOTHING,
    Middleware,
    chain,
    then,
    with_idempotency,
    with_logging,
    with_ordering,
    with_timeout,
)
from .replay import (
    HEADER_REPLAY_COUNT,
    HEADER_REPLAYED_AT,
    HEADER_REPLAYED_FROM,
    ReplayError,
    ReplayFilter,
    ReplayResult,
    replay,
)
from .requestreply import (
    DEFAULT_TIMEOUT,
    HEADER_ERROR,
    HEADER_REPLY_TO,
    Requester,
    RequestTimeoutError,
    Responder,
    ResponderError,
    serve,
)
from .routingslip import (
    HEADER_ROUTING_SLIP,
    RoutingSlip,
    Stage,
    Step,
    follow_slip,
    slip_from,
    start,
)
from .schema import (
    InMemorySchemaRegistry,
    SchemaDefinition,
    SchemaNotFoundError,
    SchemaRegistry,
    fingerprint,
)
from .sql import (
    DEFAULT_CLAIM_TIMEOUT,
    DEFAULT_IDEMPOTENCY_TABLE,
    DEFAULT_OUTBOX_TABLE,
    DEFAULT_REGISTRY_TABLE,
    DEFAULT_RETENTION,
    DbConnection,
    DbCursor,
    SqlIdempotencyStore,
    SqlOutboxStore,
    SqlSchemaRegistry,
    create_schema,
    schema_ddl,
)
from .streams import (
    DEFAULT_STREAM_PREFETCH,
    StreamOffset,
    StreamRetention,
    declare_stream,
    from_first,
    from_last,
    from_next,
    from_offset,
    from_timestamp,
    read_stream,
    stream,
)

__all__ = [
    "DEFAULT_BATCH",
    "DEFAULT_CLAIM_TIMEOUT",
    "DEFAULT_IDEMPOTENCY_TABLE",
    "DEFAULT_INTERVAL",
    "DEFAULT_MEMORY",
    "DEFAULT_OUTBOX_TABLE",
    "DEFAULT_REGISTRY_TABLE",
    "DEFAULT_RETENTION",
    "DEFAULT_STREAM_PREFETCH",
    "DEFAULT_THRESHOLD",
    "DEFAULT_TIMEOUT",
    "HEADER_ERROR",
    "HEADER_REPLAYED_AT",
    "HEADER_REPLAYED_FROM",
    "HEADER_REPLAY_COUNT",
    "HEADER_REPLY_TO",
    "HEADER_ROUTING_SLIP",
    "NOTHING",
    "ClaimCheckCodec",
    "ClaimCheckStore",
    "ConsumerGroup",
    "DbConnection",
    "DbCursor",
    "FilesystemClaimCheckStore",
    "IdempotencyStore",
    "InMemoryClaimCheckStore",
    "InMemoryIdempotencyStore",
    "InMemoryOutboxStore",
    "InMemorySchemaRegistry",
    "Middleware",
    "OutboxRecord",
    "OutboxRelay",
    "OutboxStore",
    "PartitionKey",
    "ReplayError",
    "ReplayFilter",
    "ReplayResult",
    "RequestTimeoutError",
    "Requester",
    "Responder",
    "ResponderError",
    "RoutingSlip",
    "SchemaDefinition",
    "SchemaNotFoundError",
    "SchemaRegistry",
    "SqlIdempotencyStore",
    "SqlOutboxStore",
    "SqlSchemaRegistry",
    "Stage",
    "Step",
    "StreamOffset",
    "StreamRetention",
    "by_correlation",
    "by_header",
    "chain",
    "claim_key_of",
    "create_schema",
    "declare_stream",
    "fingerprint",
    "follow_slip",
    "from_first",
    "from_last",
    "from_next",
    "from_offset",
    "from_timestamp",
    "idempotent",
    "is_claim_check",
    "ordered",
    "partition",
    "partitioned_routing_key",
    "read_stream",
    "record",
    "replay",
    "schema_ddl",
    "serve",
    "slip_from",
    "start",
    "stream",
    "then",
    "with_idempotency",
    "with_logging",
    "with_ordering",
    "with_timeout",
]
