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

from .idempotency import (
    DEFAULT_MEMORY,
    IdempotencyStore,
    InMemoryIdempotencyStore,
    idempotent,
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

__all__ = [
    "DEFAULT_BATCH",
    "DEFAULT_INTERVAL",
    "DEFAULT_MEMORY",
    "DEFAULT_TIMEOUT",
    "HEADER_ERROR",
    "HEADER_REPLY_TO",
    "IdempotencyStore",
    "InMemoryIdempotencyStore",
    "InMemoryOutboxStore",
    "OutboxRecord",
    "OutboxRelay",
    "OutboxStore",
    "RequestTimeoutError",
    "Requester",
    "Responder",
    "ResponderError",
    "idempotent",
    "record",
    "serve",
]
