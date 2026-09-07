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

"""Where a message goes when it cannot be handled.

These names are convention rather than protocol, which is exactly why they have
to be identical everywhere: an operator looking for the dead letters of
``orders.new`` should find them in ``orders.new.dlq`` whether the consumer that
gave up was written in Java, Go, .NET or Python.
"""

from __future__ import annotations

from datetime import timedelta

#: Where a message goes when every attempt has been used.
DEAD_LETTER_SUFFIX = ".dlq"

#: Where a message goes when a person has to look at it.
PARKED_SUFFIX = ".parked"


def dead_letter_queue(queue: str) -> str:
    """``orders.new`` becomes ``orders.new.dlq``."""
    return queue + DEAD_LETTER_SUFFIX


def parked_queue(queue: str) -> str:
    """``orders.new`` becomes ``orders.new.parked``."""
    return queue + PARKED_SUFFIX


def retry_queue(queue: str, delay: timedelta) -> str:
    """``orders.new`` and 30s become ``orders.new.retry.30s``.

    The delay is in the name because a delay queue is per-delay: its
    ``x-message-ttl`` is fixed at declaration, so a policy with four different
    waits needs four queues, and an operator should be able to tell which is
    which without reading its arguments.
    """
    return f"{queue}.retry.{_short(delay)}"


def _short(delay: timedelta) -> str:
    """A duration as the shortest thing that reads as one: 30s, 5m, 2h."""
    seconds = int(delay.total_seconds())
    if seconds <= 0:
        return "0s"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"
