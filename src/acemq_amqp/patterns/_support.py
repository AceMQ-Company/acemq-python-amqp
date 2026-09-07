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

"""The bit every wrapping pattern needs and none of them should own.

A pattern that wraps a handler has to accept both shapes the library accepts,
because the whole point of a wrapper is that the thing inside it does not know
it is wrapped. Written once here rather than three lines at a time in every
module that wraps something.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import TypeVar

from ..connection import Message

T = TypeVar("T")


async def decide(handler: Callable[[Message], T | Awaitable[T]], message: Message) -> T:
    """Runs a function of either shape over a message and returns its answer.

    :param handler: a coroutine function or a plain one
    :param message: what to hand it
    :returns: what it produced — an acknowledgement for a handler, a payload for
        a routing-slip step
    """
    returned = handler(message)
    return await returned if inspect.isawaitable(returned) else returned
