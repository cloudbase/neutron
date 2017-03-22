# Copyright 2017 Cloudbase Solutions.
# All Rights Reserved.
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
import eventlet
from eventlet import tpool


def avoid_blocking_call(f, *args, **kwargs):
    """Ensures that the invoked method will not block other greenthreads.
    Performs the call in a different thread using tpool.execute when called
    from a greenthread.
    """
    # Note that eventlet.getcurrent will always return a greenlet object.
    # In case of a greenthread, the parent greenlet will always be the hub
    # loop greenlet.
    if eventlet.getcurrent().parent:
        return tpool.execute(f, *args, **kwargs)
    else:
        return f(*args, **kwargs)


class NamedPipe(object):
    def __init__(self, pipe_name):
        self._read = winutils.pywintypes.OVERLAPPED()
        self._read.hEvent = winutils.get_new_event()
        self._write = winutils.pywintypes.OVERLAPPED()
        self._write.hEvent = winutils.get_new_event()
        self._connect = winutils.pywintypes.OVERLAPPED()
        self._connect.hEvent = winutils.get_new_event()
        self._read_pending = False
        self.name = winutils.get_pipe_name("TransactionQueuePipeName")
        self.namedpipe = winutils.create_named_pipe(self.name)
        self._npipe_file = None
        if not self.namedpipe:
            raise Exception("Failed to create named pipe.")

    @property
    def read_overlapped_event(self):
        return self._read.hEvent

    @property
    def write_overlapped_event(self):
        return self._write.hEvent

    def _wait_read(self, timeout=winutils.win32event.INFINITE):
        winutils.win32event.WaitForMultipleObjects(
            [self._read.hEvent],
            False,
            winutils.win32event.INFINITE)

    def _wait_write(self, timeout=winutils.win32event.INFINITE):
        winutils.win32event.WaitForMultipleObjects(
            [self._write.hEvent],
            False,
            winutils.win32event.INFINITE)

    def _wait_connect(self, timeout=winutils.win32event.INFINITE):
        winutils.win32event.WaitForMultipleObjects(
            [self._connect.hEvent],
            False,
            winutils.win32event.INFINITE)

    def blocking_write(self, buf):
        encoded_buf = winutils.get_encoded_buffer(buf)
        (errCode, nBytesWritten) = winutils.write_file(self.namedpipe,
                                                       encoded_buf,
                                                       self._write)
        if errCode:
            if errCode == winutils.winerror.ERROR_IO_PENDING:
                # Wait infinite for the write to be finished
                avoid_blocking_call(self._wait_write)
            else:
                raise Exception("Could not write to named pipe. "
                                "errCode: '%s'" % errCode)

    def nonblocking_read(self, bytes_to_read):
        if self._npipe_file is None:
            raise Exception("create_file must be called first")
        if not self._read_pending:
            (errCode, self._read_buffer) = winutils.read_file(self._npipe_file,
                                                              bytes_to_read,
                                                              self._read)
            if errCode:
                if errCode == winutils.winerror.ERROR_IO_PENDING:
                    self._read_pending = True
                else:
                    raise Exception("Could not read from named pipe. "
                                    "errCode: '%s'" % errCode)

    def get_read_result(self):
        if self._read_pending:
            try:
                nBytesRead = winutils.get_overlapped_result(
                    self._npipe_file, self._read, False)
                self._read_pending = False
                return self._read_buffer[:nBytesRead]
            except winutils.pywintypes.error as e:
                if e.winerror == winutils.winerror.ERROR_IO_INCOMPLETE:
                    # Ignore the exception, will try to get the result
                    # again in the next loop iteration.
                    self._read_pending = True
                else:
                    # Any other exception means that something happened to the
                    # pipe, which should not be our case.
                    raise Exception("Error when retrieving the read result. "
                                    "Error: %s" % e)

    def connect(self):
        errCode = winutils.connect_named_pipe(self.namedpipe, self._connect)
        if errCode:
            if errCode == winutils.winerror.ERROR_IO_PENDING:
                # A call to "wait_for_connection" is necessary in this case to
                # wait for the connection to be completed
                return
            else:
                raise Exception("Could not call connect to named pipe, "
                                "errCode: '%s'" % errCode)

    def wait_for_connection(self):
        avoid_blocking_call(self._wait_connect)

    def wait_for_read(self):
        if self._read_pending:
            avoid_blocking_call(self._wait_read)

    def create_file(self):
        try:
            self._npipe_file = winutils.create_file(self.name)
            try:
                winutils.set_pipe_mode(self._npipe_file,
                                       winutils.win32pipe.PIPE_READMODE_BYTE)
            except winutils.pywintypes.error as e:
                raise Exception("Could not set pipe read mode to byte. "
                                "Error: %s" % e)
        except winutils.pywintypes.error as e:
            raise Exception("Could not create file for named pipe. "
                            "Error: %s" % e)
