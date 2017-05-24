# Copyright 2017 Cloudbase Solutions Srl
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


from ovs import winutils

from neutron.agent.ovsdb.native.common import base_connection
from neutron.agent.ovsdb.native.windows import utils


class TransactionQueue(base_connection.TransactionQueue):
    def _init_alert_notification(self):
        # We will use an event to get signaled when there is something in
        # the queue. The OVS poller can wait on events on Windows.
        # NOTE(abalutoiu) The assumption made is that the queue has
        # length 1, otherwise we will need to have a list of events with
        # the size of the queue.
        self.alert_event = winutils.get_new_event(bManualReset=True,
                                                  bInitialState=False)

    def _alert_notification_consume(self):
        # Set the event object to the nonsignaled state to indicate that
        # the queue is empty.
        winutils.win32event.ResetEvent(self.alert_event)

    def _alert_notify(self):
        # Set the event object to the signaled state to indicate that
        # we have something in the queue.
        winutils.win32event.SetEvent(self.alert_event)

    @property
    def alert_fileno(self):
        return self.alert_event


class Connection(base_connection.Connection):

    def _get_transaction_queue(self, size):
        return TransactionQueue(1)

    def _poller_block(self):
        # Ensure that WaitForMultipleObjects will not block other greenthreads.
        # poller.block uses WaitForMultipleObjects on Windows
        utils.avoid_blocking_call(self.poller.block)
