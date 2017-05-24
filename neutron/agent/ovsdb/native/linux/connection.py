# Copyright (c) 2015 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os

import six

from neutron.agent.ovsdb.native.common import base_connection


class TransactionQueue(base_connection.TransactionQueue):
    def _init_alert_notification(self):
        alertpipe = os.pipe()
        # NOTE(ivasilevskaya) python 3 doesn't allow unbuffered I/O.
        # Will get around this constraint by using binary mode.
        self.alertin = os.fdopen(alertpipe[0], 'rb', 0)
        self.alertout = os.fdopen(alertpipe[1], 'wb', 0)

    def _alert_notification_consume(self):
        self.alertin.read(1)

    def _alert_notify(self):
        self.alertout.write(six.b('X'))
        self.alertout.flush()

    @property
    def alert_fileno(self):
        return self.alertin.fileno()


class Connection(base_connection.Connection):

    def _get_transaction_queue(self, size):
        return TransactionQueue(1)

    def _poller_block(self):
        self.poller.block()
