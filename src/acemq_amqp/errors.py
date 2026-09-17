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

"""What this library raises when it will not do something.

Deliberately few. Most of what goes wrong with a broker is the broker's own
exception, and wrapping every one of those in a class of ours would hide the
detail somebody actually needs while adding a name they then have to learn.
These exist for the failures that are ours rather than aio-pika's.

:class:`~acemq_amqp.FatalError` is not here: it lives with the acknowledgement
model in :mod:`acemq_amqp.ack`, because it is a statement about a message rather
than about this library.
"""

from __future__ import annotations


class AceMQError(Exception):
    """Something this library refused to do."""


class SecurityError(AceMQError):
    """A security setting that cannot be honoured.

    Raised before anything is dialled rather than after: a missing certificate
    file, an environment variable that was going to hold the password, or TLS
    settings against a URL that is not encrypted are all things to hear about at
    start-up. A connection that half-applied them and carried on is the failure
    this exists to prevent.
    """


class PublishError(AceMQError):
    """A message the broker would not take, or would not route.

    It carries where the message was going, which is the first thing anybody
    wants when a routing key was built from a variable.

    :param message_id: the envelope identifier it went out with
    :param exchange: where it was sent
    :param routing_key: what it was sent under
    :param reason: the broker's explanation
    :param unroutable: true when the message reached no queue rather than being
        refused. The broker was working; nothing was listening
    :param summary: the whole message, in place of the sentence composed from
        the arguments above. For a failure that is about a batch rather than
        about one message — :meth:`acemq_amqp.Publisher.send_all` raises one of
        these naming how many messages were not confirmed and how many were,
        in the words Java and .NET use for the same failure, so that one
        runbook covers all of them. A batch failure leaves ``message_id``
        empty, because there is no single message it is about
    """

    def __init__(
        self,
        message_id: str,
        exchange: str,
        routing_key: str,
        reason: str,
        *,
        unroutable: bool = False,
        summary: str = "",
    ) -> None:
        self.message_id = message_id
        self.exchange = exchange
        self.routing_key = routing_key
        self.reason = reason
        self.unroutable = unroutable

        where = f"exchange {exchange!r} with key {routing_key!r}"
        if summary:
            super().__init__(summary)
        elif unroutable:
            super().__init__(
                f"acemq: message {message_id} to {where} reached no queue: {reason}"
            )
        else:
            super().__init__(f"acemq: cannot publish message {message_id} to {where}: {reason}")
