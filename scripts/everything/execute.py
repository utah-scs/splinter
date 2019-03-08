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

import argparse
import subprocess
import pprint
import logging
import sys
import time
import os
import xml.etree.ElementTree as ET
import multiprocessing.pool as pool

# local imports
import logpipe as lp


class SubprocessException(Exception):
    def __init__(self, cmd, returnCode):
        self.cmd = cmd
        self.returnCode = returnCode


class RemoteSubprocessException(SubprocessException):
    def __init__(self, host, cmd, returnCode):
        self.host = host
        super(RemoteSubprocessException, self).__init__(cmd, returnCode)


def execute(cmd, logger):
    outPipe = lp.LogPipe(logger, logging.INFO)
    errPipe = lp.LogPipe(logger, logging.ERROR)
    process = subprocess.run(cmd,
                               shell=True,
                               stdin=subprocess.PIPE,
                               stdout=outPipe,
                               stderr=errPipe,
                               close_fds=True)
    outPipe.close()
    errPipe.close()

    if process.returncode:
        raise SubprocessException(cmd, process.returncode)