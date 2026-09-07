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

"""The bits every wrapping pattern needs and none of them should own.

A pattern that wraps a handler has to accept both shapes of handler the library
accepts, because the whole point of a wrapper is that the thing inside it does
not know it is wrapped. Written once here rather than three lines at a time in
every module that wraps something.
"""

from __future__ import annotations

import inspect

from ..ack import Ack
from ..connection import Handler, Message


async def decide(handler: Handler, message: Message) -> Ack:
    """Runs a handler of either shape and returns its decision.

    :param handler: a coroutine function or a plain one
    :param message: what to hand it
    :returns: what it decided
    """
    returned = handler(message)
    return await returned if inspect.isawaitable(returned) else returned
