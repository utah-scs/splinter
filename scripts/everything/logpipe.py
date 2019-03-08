#!/usr/bin/python
#
# Copyright (c) 2018 University of Utah
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


import logging
import threading
import os
import subprocess

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

class LogPipe(threading.Thread):

    def __init__(self, logger, level):
        threading.Thread.__init__(self)

        self.__logger = logger
        self.__level = level

        self.daemon = False
        self.fdRead, self.fdWrite = os.pipe()
        self.pipeReader = os.fdopen(self.fdRead)
        self.start()


    # offers the file descriptor so the subprocess can write to the file
    def fileno(self):
        return self.fdWrite

    # called by threading.Thread.start(), allows the Logpipe to forward information written to the logger
    def run(self):
        for line in iter(self.pipeReader.readline, ''):
            self.__logger.log(self.__level, line.strip('\n'))

        self.pipeReader.close()

    # cleanup of the logPipe
    def close(self):
        os.close(self.fdWrite)