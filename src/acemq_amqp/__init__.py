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
"""

from __future__ import annotations

from . import headers
from .ack import Ack, Action, FatalError, accept, reject, retry
from .envelope import Envelope
from .naming import dead_letter_queue, parked_queue, retry_queue
from .retry import RetryPolicy, exponential_retry, fixed_retry, no_retry

__version__ = "0.1.0"

__all__ = [
    "Ack",
    "Action",
    "Envelope",
    "FatalError",
    "RetryPolicy",
    "__version__",
    "accept",
    "dead_letter_queue",
    "exponential_retry",
    "fixed_retry",
    "headers",
    "no_retry",
    "parked_queue",
    "reject",
    "retry",
    "retry_queue",
]
